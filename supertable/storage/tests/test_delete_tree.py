"""Regression tests for AUDIT_BUGS C2 at the storage layer.

Two things are under test here:

1. :class:`FakeObjectStore` itself.  It is the instrument every C2 test is
   measured with, so if it stops modelling the trap (``exists(prefix)`` being
   False over a bucket full of data) those tests go quietly green for the
   wrong reason.  The guards in :class:`TestFakeObjectStoreSemantics` pin the
   behaviours that make it different from ``MagicMock()``.

2. ``StorageInterface.delete_tree`` — the prefix-listed wipe that replaced the
   ``if storage.exists(folder): storage.delete(folder)`` guard.
"""

from __future__ import annotations

import os
import shutil
import tempfile

import pytest

from supertable.storage.local_storage import LocalStorage
from supertable.storage.tests.fake_object_store import FakeObjectStore


def _table_bucket(base_prefix: str = "") -> FakeObjectStore:
    """A bucket holding one table's worth of keys under org/sup/tables/t."""
    return FakeObjectStore(base_prefix=base_prefix).seed(
        "org/sup/tables/t/snapshots/v0.json",
        "org/sup/tables/t/snapshots/v1.json",
        "org/sup/tables/t/data/year=2026/month=09/part-0.parquet",
        "org/sup/tables/t/data/year=2026/month=09/part-1.parquet",
        "org/sup/tables/t/data/year=2026/month=10/part-2.parquet",
    )


# ===========================================================================
# The double must model the trap
# ===========================================================================

class TestFakeObjectStoreSemantics:

    def test_exists_is_false_for_a_prefix_that_holds_objects(self):
        """The C2 trap itself: a folder is not an object.

        ``MagicMock().exists()`` answers truthy for this, which is exactly
        why a ~2,700-test suite never noticed the drop-table bug.
        """
        store = _table_bucket()
        assert store.exists("org/sup/tables/t") is False
        assert store.exists("org/sup/tables/t/data") is False
        # ...while the bucket is demonstrably full.
        assert len(store.keys_under("org/sup/tables/t")) == 5

    def test_exists_is_true_only_for_an_exact_key(self):
        store = _table_bucket()
        assert store.exists("org/sup/tables/t/snapshots/v0.json") is True
        assert store.exists("org/sup/tables/t/snapshots/v0") is False
        assert store.exists("org/sup/tables/t/snapshots/v0.json/") is True  # stripped

    def test_delete_on_a_prefix_removes_nothing_and_raises(self):
        store = _table_bucket()
        with pytest.raises(FileNotFoundError):
            store.delete("org/sup/tables/t")
        assert len(store.keys()) == 5, "a prefix delete must not touch any key"

    def test_delete_on_an_exact_key_removes_exactly_that_key(self):
        store = _table_bucket()
        store.delete("org/sup/tables/t/snapshots/v0.json")
        assert "org/sup/tables/t/snapshots/v0.json" not in store.keys()
        assert len(store.keys()) == 4

    def test_list_files_is_one_delimited_level_prefix_inclusive(self):
        store = _table_bucket()
        assert store.list_files("org/sup/tables/t") == [
            "org/sup/tables/t/data",       # common prefix
            "org/sup/tables/t/snapshots",  # common prefix
        ]
        assert store.list_files("org/sup/tables/t/snapshots") == [
            "org/sup/tables/t/snapshots/v0.json",
            "org/sup/tables/t/snapshots/v1.json",
        ]

    def test_list_files_bakes_in_base_prefix_like_the_real_backends(self):
        """M4's asymmetry, reproduced on purpose.

        ``list_files`` applies ``base_prefix`` on the way in and leaves it in
        the result, while every other method re-applies it — so feeding a
        listing back into the store double-prefixes.  ``delete_tree`` must not
        rely on that round trip.
        """
        store = _table_bucket(base_prefix="lakehouse")
        listed = store.list_files("org/sup/tables/t/snapshots")
        assert listed == [
            "lakehouse/org/sup/tables/t/snapshots/v0.json",
            "lakehouse/org/sup/tables/t/snapshots/v1.json",
        ]
        # Chained straight back in, it addresses lakehouse/lakehouse/... :
        assert store.exists(listed[0]) is False

    def test_get_directory_structure_is_recursive_and_relative(self):
        store = _table_bucket(base_prefix="lakehouse")
        struct = store.get_directory_structure("org/sup/tables/t")
        assert struct == {
            "data": {
                "year=2026": {
                    "month=09": {"part-0.parquet": None, "part-1.parquet": None},
                    "month=10": {"part-2.parquet": None},
                },
            },
            "snapshots": {"v0.json": None, "v1.json": None},
        }


# ===========================================================================
# delete_tree
# ===========================================================================

class TestDeleteTree:

    def test_removes_every_key_under_the_prefix(self):
        store = _table_bucket()
        store.seed("org/sup/tables/other/data/keep.parquet")

        removed = store.delete_tree("org/sup/tables/t")

        assert removed == 5
        assert store.keys_under("org/sup/tables/t") == []
        assert store.keys() == ["org/sup/tables/other/data/keep.parquet"], \
            "a sibling table must survive the drop"

    def test_returns_zero_when_the_prefix_lists_nothing(self):
        """"Already gone" is a listing result, never an inference."""
        store = FakeObjectStore()
        assert store.delete_tree("org/sup/tables/ghost") == 0

    def test_removes_path_when_it_is_itself_an_exact_key(self):
        store = FakeObjectStore().seed("org/sup/staging/idx_files.json")
        assert store.delete_tree("org/sup/staging/idx_files.json") == 1
        assert store.keys() == []

    def test_works_under_a_base_prefix(self):
        """M4 must not defeat the C2 fix on a prefixed bucket."""
        store = _table_bucket(base_prefix="lakehouse")

        removed = store.delete_tree("org/sup/tables/t")

        assert removed == 5
        assert store.keys() == []

    def test_a_failed_delete_propagates(self):
        """The caller must never reach its catalog delete on a partial wipe."""
        store = _table_bucket()
        real_delete = store.delete

        def flaky(path: str) -> None:
            if path.endswith("part-1.parquet"):
                raise PermissionError("403 AccessDenied")
            real_delete(path)

        store.delete = flaky  # type: ignore[method-assign]

        with pytest.raises(PermissionError):
            store.delete_tree("org/sup/tables/t")

    def test_concurrent_deletion_of_a_listed_key_is_tolerated(self):
        """Losing a race still satisfies the post-condition (the key is gone)."""
        store = _table_bucket()
        real_delete = store.delete

        def racy(path: str) -> None:
            if path.endswith("part-1.parquet"):
                raise FileNotFoundError(path)
            real_delete(path)

        store.delete = racy  # type: ignore[method-assign]

        removed = store.delete_tree("org/sup/tables/t")
        assert removed == 4


class TestDeleteTreeLocalParity:
    """LOCAL is where the old guard accidentally worked; keep it working."""

    def setup_method(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = LocalStorage()
        for rel in (
            "org/sup/tables/t/snapshots/v0.json",
            "org/sup/tables/t/data/year=2026/part-0.parquet",
            "org/sup/tables/t/data/year=2026/part-1.parquet",
        ):
            self.storage.write_text(os.path.join(self.tmp, rel), "x")

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_removes_the_whole_directory_tree(self):
        folder = os.path.join(self.tmp, "org/sup/tables/t")

        removed = self.storage.delete_tree(folder)

        assert removed == 3
        assert not os.path.exists(folder), "the directory itself must be gone too"

    def test_missing_directory_returns_zero(self):
        assert self.storage.delete_tree(os.path.join(self.tmp, "nope")) == 0

    def test_removes_an_exact_file(self):
        f = os.path.join(self.tmp, "org/sup/tables/t/snapshots/v0.json")
        assert self.storage.delete_tree(f) == 1
        assert not os.path.exists(f)
