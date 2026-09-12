"""Regression tests for AUDIT_BUGS C2 (and H7) at the table level.

Dropping a table, a supertable or a staging area used to wipe storage behind
an ``if self.storage.exists(folder)`` guard.  On S3/MinIO/Azure/GCS a folder
is not an object, so ``exists()`` is a HEAD on a key that was never created
and answers ``False`` for a prefix holding the entire table.  The wipe was
skipped, the catalog entry was removed anyway, and the log claimed the storage
had been deleted — every parquet file orphaned and billed, with nothing left
pointing at it.

These tests run against :class:`FakeObjectStore`, which models that HEAD
semantics.  Against ``MagicMock()`` — whose ``exists()`` is truthy for any
argument — every one of them passes with the bug still in place.
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from supertable.storage.tests.fake_object_store import FakeObjectStore


_TABLE_KEYS = (
    "org/sup/tables/events/snapshots/v0.json",
    "org/sup/tables/events/snapshots/v1.json",
    "org/sup/tables/events/data/year=2026/month=09/part-0.parquet",
    "org/sup/tables/events/data/year=2026/month=09/part-1.parquet",
    "org/sup/tables/events/data/_tombstone/dv-0.parquet",
)


# ---------------------------------------------------------------------------
# Builders (mirror the __new__ style used by the existing suites)
# ---------------------------------------------------------------------------

def _simple_table(storage, catalog=None, simple_name="events"):
    from supertable.simple_table import SimpleTable

    super_table = MagicMock()
    super_table.organization = "org"
    super_table.super_name = "sup"
    super_table.storage = storage

    obj = SimpleTable.__new__(SimpleTable)
    obj.super_table = super_table
    obj.identity = "tables"
    obj.simple_name = simple_name
    obj.storage = storage
    obj.catalog = catalog or MagicMock()
    obj.simple_dir = f"org/sup/tables/{simple_name}"
    obj.data_dir = f"{obj.simple_dir}/data"
    obj.snapshot_dir = f"{obj.simple_dir}/snapshots"
    return obj


def _super_table(storage, catalog=None):
    from supertable.super_table import SuperTable

    obj = SuperTable.__new__(SuperTable)
    obj.identity = "super"
    obj.super_name = "sup"
    obj.organization = "org"
    obj.storage = storage
    obj.catalog = catalog or MagicMock()
    obj.super_dir = "org/sup/super"
    return obj


def _staging(storage, catalog=None):
    from supertable.staging_area import Staging

    obj = Staging.__new__(Staging)
    obj.organization = "org"
    obj.super_name = "sup"
    obj.storage = storage
    obj.catalog = catalog or MagicMock()
    obj.staging_name = "inbox"
    obj._is_manager = False
    obj.base_staging_dir = "org/sup/staging"
    obj.stage_dir = "org/sup/staging/inbox"
    obj.files_index_path = "org/sup/staging/inbox_files.json"
    # Run the critical section inline; the Redis lock is not under test.
    obj._with_lock = lambda fn: fn()
    return obj


# ===========================================================================
# SimpleTable.delete
# ===========================================================================

class TestSimpleTableDropWipesStorage:

    @patch("supertable.simple_table.check_write_access")
    def test_every_object_under_the_table_is_deleted(self, _access):
        storage = FakeObjectStore().seed(*_TABLE_KEYS)
        catalog = MagicMock()
        table = _simple_table(storage, catalog)

        table.delete(role_name="admin")

        assert storage.keys_under("org/sup/tables/events") == [], \
            "dropping a table must leave nothing behind on object storage"
        catalog.delete_simple_table.assert_called_once_with("org", "sup", "events")

    @patch("supertable.simple_table.check_write_access")
    def test_sibling_tables_are_untouched(self, _access):
        storage = FakeObjectStore().seed(
            *_TABLE_KEYS,
            "org/sup/tables/orders/data/part-0.parquet",
            "org/sup/tables/events_archive/data/part-0.parquet",
        )
        table = _simple_table(storage)

        table.delete(role_name="admin")

        assert storage.keys() == [
            "org/sup/tables/events_archive/data/part-0.parquet",
            "org/sup/tables/orders/data/part-0.parquet",
        ], "the wipe must be bounded by the table prefix, name-prefix siblings included"

    @patch("supertable.simple_table.check_write_access")
    def test_catalog_pointer_survives_a_failed_wipe(self, _access):
        """A table whose data still exists must stay findable."""
        storage = FakeObjectStore().seed(*_TABLE_KEYS)
        real_delete = storage.delete

        def flaky(path):
            if path.endswith("part-1.parquet"):
                raise PermissionError("403 AccessDenied")
            real_delete(path)

        storage.delete = flaky  # type: ignore[method-assign]
        catalog = MagicMock()
        table = _simple_table(storage, catalog)

        with pytest.raises(PermissionError):
            table.delete(role_name="admin")

        catalog.delete_simple_table.assert_not_called()

    @patch("supertable.simple_table.check_write_access")
    def test_already_empty_prefix_still_drops_the_catalog_entry(self, _access):
        """Listing nothing is the one reading of "already gone" that is safe."""
        storage = FakeObjectStore()
        catalog = MagicMock()
        table = _simple_table(storage, catalog)

        table.delete(role_name="admin")

        catalog.delete_simple_table.assert_called_once()

    @patch("supertable.simple_table.check_write_access")
    def test_rbac_denial_still_precedes_any_deletion(self, mock_access):
        mock_access.side_effect = PermissionError("denied")
        storage = FakeObjectStore().seed(*_TABLE_KEYS)
        catalog = MagicMock()
        table = _simple_table(storage, catalog)

        with pytest.raises(PermissionError):
            table.delete(role_name="viewer")

        assert len(storage.keys()) == len(_TABLE_KEYS)
        catalog.delete_simple_table.assert_not_called()


# ===========================================================================
# SuperTable.delete
# ===========================================================================

class TestSuperTableDropWipesStorage:

    def test_every_object_under_the_supertable_is_deleted(self):
        storage = FakeObjectStore().seed(
            *_TABLE_KEYS,
            "org/sup/super/meta.json",
            "org/sup/staging/inbox/raw.parquet",
        )
        catalog = MagicMock()
        st = _super_table(storage, catalog)

        st.delete(role_name="admin")

        assert storage.keys() == []
        catalog.delete_super_table.assert_called_once_with("org", "sup")

    def test_other_supertables_are_untouched(self):
        storage = FakeObjectStore().seed(
            "org/sup/super/meta.json",
            "org/sup2/super/meta.json",
            "org2/sup/super/meta.json",
        )
        st = _super_table(storage)

        st.delete(role_name="admin")

        assert storage.keys() == ["org/sup2/super/meta.json", "org2/sup/super/meta.json"]

    def test_redis_meta_survives_a_failed_wipe(self):
        """The invariant stated at super_table.py's own delete(), now upheld."""
        storage = FakeObjectStore().seed(*_TABLE_KEYS)
        storage.delete = MagicMock(side_effect=OSError("bucket unreachable"))
        catalog = MagicMock()
        st = _super_table(storage, catalog)

        with pytest.raises(OSError):
            st.delete(role_name="admin")

        catalog.delete_super_table.assert_not_called()


# ===========================================================================
# Staging.delete  (C2 + H7)
# ===========================================================================

class TestStagingDropWipesStorage:

    @patch("supertable.staging_area.check_write_access")
    def test_stage_folder_and_index_are_both_deleted(self, _access):
        """H7: the old code called ``storage.delete_recursive`` — a method no
        backend implements, so on LOCAL (the one place the ``exists`` guard
        was True) this raised AttributeError, and on object storage the guard
        skipped it entirely."""
        storage = FakeObjectStore().seed(
            "org/sup/staging/inbox/a.parquet",
            "org/sup/staging/inbox/b.parquet",
            "org/sup/staging/inbox_files.json",
            "org/sup/staging/other/c.parquet",
        )
        catalog = MagicMock()
        stage = _staging(storage, catalog)

        stage.delete(role_name="admin")

        assert storage.keys() == ["org/sup/staging/other/c.parquet"], \
            "the stage folder and its sibling index must go; other stages must not"
        catalog.delete_staging_meta.assert_called_once_with("org", "sup", "inbox")

    @patch("supertable.staging_area.check_write_access")
    def test_redis_meta_survives_a_failed_wipe(self, _access):
        storage = FakeObjectStore().seed("org/sup/staging/inbox/a.parquet")
        storage.delete = MagicMock(side_effect=OSError("bucket unreachable"))
        catalog = MagicMock()
        stage = _staging(storage, catalog)

        with pytest.raises(OSError):
            stage.delete(role_name="admin")

        catalog.delete_staging_meta.assert_not_called()
