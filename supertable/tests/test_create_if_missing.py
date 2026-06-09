"""
Tests for the ``create_if_missing`` opt-out on SuperTable + SimpleTable
constructors.

The read path (DataReader, MetaReader, DataEstimator) must never mint a
catalog entry as a side effect of constructing the Python object. These
tests verify:

  1. SuperTable
     - create_if_missing=True (default) — bootstrap when missing
     - create_if_missing=False — raise SuperTableNotFoundError when missing
     - create_if_missing=False — happy path (root exists) still works
     - Reserved names still rejected with ValueError, regardless of flag
  2. SimpleTable
     - create_if_missing=True (default) — bootstrap when missing
     - create_if_missing=False — raise TableNotFoundError when missing
     - create_if_missing=False — happy path (leaf exists) still works
  3. Side-effect guarantee
     - When create_if_missing=False raises, no storage mkdirs, no Redis
       ensure_root, no RBAC scaffolding, no leaf write happens.
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable.errors import (  # noqa: E402
    SuperTableNotFoundError,
    TableNotFoundError,
)


# ---------------------------------------------------------------------------
# Patch targets — SuperTable
# ---------------------------------------------------------------------------

_SUP_MOD = "supertable.super_table"
_P_SUP_GET_STORAGE = f"{_SUP_MOD}.get_storage"
_P_SUP_REDIS_CAT = f"{_SUP_MOD}.RedisCatalog"
_P_SUP_ROLE_MGR = f"{_SUP_MOD}.RoleManager"
_P_SUP_USER_MGR = f"{_SUP_MOD}.UserManager"


# ---------------------------------------------------------------------------
# Patch targets — SimpleTable
# ---------------------------------------------------------------------------

_SIMPLE_MOD = "supertable.simple_table"
_P_SIMPLE_REDIS_CAT = f"{_SIMPLE_MOD}.RedisCatalog"
_P_SIMPLE_GEN_FILENAME = f"{_SIMPLE_MOD}.generate_filename"


# ===========================================================================
# 1. SuperTable
# ===========================================================================

class TestSuperTableCreateIfMissing:

    @patch(_P_SUP_USER_MGR)
    @patch(_P_SUP_ROLE_MGR)
    @patch(_P_SUP_REDIS_CAT)
    @patch(_P_SUP_GET_STORAGE)
    def test_default_creates_when_missing(
        self, mock_storage, MockCat, MockRole, MockUser,
    ):
        """Default behaviour (create_if_missing=True) bootstraps a
        missing supertable — this is the writer path."""
        from supertable.super_table import SuperTable

        stor = MagicMock()
        mock_storage.return_value = stor
        cat = MagicMock()
        cat.root_exists.return_value = False  # missing
        MockCat.return_value = cat

        st = SuperTable("new_sup", "org")

        # Bootstrap happened
        stor.makedirs.assert_called_once()
        cat.ensure_root.assert_called_once_with("org", "new_sup")
        MockRole.assert_called_once()
        MockUser.assert_called_once()
        assert st.super_name == "new_sup"

    @patch(_P_SUP_USER_MGR)
    @patch(_P_SUP_ROLE_MGR)
    @patch(_P_SUP_REDIS_CAT)
    @patch(_P_SUP_GET_STORAGE)
    def test_explicit_true_creates_when_missing(
        self, mock_storage, MockCat, MockRole, MockUser,
    ):
        """Explicit create_if_missing=True is identical to the default."""
        from supertable.super_table import SuperTable

        stor = MagicMock()
        mock_storage.return_value = stor
        cat = MagicMock()
        cat.root_exists.return_value = False
        MockCat.return_value = cat

        SuperTable("new_sup", "org", create_if_missing=True)

        cat.ensure_root.assert_called_once_with("org", "new_sup")

    @patch(_P_SUP_USER_MGR)
    @patch(_P_SUP_ROLE_MGR)
    @patch(_P_SUP_REDIS_CAT)
    @patch(_P_SUP_GET_STORAGE)
    def test_false_raises_when_missing(
        self, mock_storage, MockCat, MockRole, MockUser,
    ):
        """create_if_missing=False raises SuperTableNotFoundError instead
        of bootstrapping."""
        from supertable.super_table import SuperTable

        stor = MagicMock()
        mock_storage.return_value = stor
        cat = MagicMock()
        cat.root_exists.return_value = False
        MockCat.return_value = cat

        with pytest.raises(SuperTableNotFoundError) as exc_info:
            SuperTable("ghost_sup", "ghost_org", create_if_missing=False)

        # Error carries the coordinates
        assert exc_info.value.organization == "ghost_org"
        assert exc_info.value.super_name == "ghost_sup"
        assert "ghost_org/ghost_sup" in str(exc_info.value)

        # And no side effects happened
        stor.makedirs.assert_not_called()
        cat.ensure_root.assert_not_called()
        MockRole.assert_not_called()
        MockUser.assert_not_called()

    @patch(_P_SUP_USER_MGR)
    @patch(_P_SUP_ROLE_MGR)
    @patch(_P_SUP_REDIS_CAT)
    @patch(_P_SUP_GET_STORAGE)
    def test_false_happy_path_when_root_exists(
        self, mock_storage, MockCat, MockRole, MockUser,
    ):
        """When the supertable does exist, create_if_missing=False is a
        no-op — same fast path as the default."""
        from supertable.super_table import SuperTable

        stor = MagicMock()
        mock_storage.return_value = stor
        cat = MagicMock()
        cat.root_exists.return_value = True  # exists
        MockCat.return_value = cat

        st = SuperTable("exists_sup", "org", create_if_missing=False)

        assert st.super_name == "exists_sup"
        cat.root_exists.assert_called_once_with("org", "exists_sup")
        # Fast path: no bootstrap, no RBAC scaffolding
        stor.makedirs.assert_not_called()
        cat.ensure_root.assert_not_called()
        MockRole.assert_not_called()
        MockUser.assert_not_called()

    @patch(_P_SUP_USER_MGR)
    @patch(_P_SUP_ROLE_MGR)
    @patch(_P_SUP_REDIS_CAT)
    @patch(_P_SUP_GET_STORAGE)
    def test_reserved_name_rejected_regardless_of_flag(
        self, mock_storage, MockCat, MockRole, MockUser,
    ):
        """Reserved names raise ValueError before create_if_missing is
        even consulted."""
        from supertable.super_table import SuperTable

        with pytest.raises(ValueError):
            SuperTable("_sentinel_", "org", create_if_missing=False)
        with pytest.raises(ValueError):
            SuperTable("_sentinel_", "org", create_if_missing=True)


# ===========================================================================
# 2. SimpleTable
# ===========================================================================

def _mock_super_table(org: str = "org", sup: str = "sup") -> MagicMock:
    """A MagicMock SuperTable suitable for SimpleTable __init__."""
    st = MagicMock()
    st.organization = org
    st.super_name = sup
    st.storage = MagicMock()
    return st


class TestSimpleTableCreateIfMissing:

    @patch(_P_SIMPLE_GEN_FILENAME)
    @patch(_P_SIMPLE_REDIS_CAT)
    def test_default_creates_when_missing(self, MockCat, mock_filename):
        """Default behaviour (create_if_missing=True) bootstraps a
        missing simple table — this is the writer path."""
        from supertable.simple_table import SimpleTable

        cat = MagicMock()
        cat.leaf_exists.return_value = False  # missing
        MockCat.return_value = cat
        mock_filename.return_value = "snap_v0.json"

        parent = _mock_super_table()
        parent.storage.exists.return_value = False  # so makedirs runs

        st = SimpleTable(parent, "new_table")

        # Bootstrap happened: mkdirs + initial snapshot write + CAS
        assert parent.storage.makedirs.call_count >= 1
        parent.storage.write_json.assert_called_once()
        assert cat.set_leaf_payload_cas.called or cat.set_leaf_path_cas.called
        assert st.simple_name == "new_table"

    @patch(_P_SIMPLE_REDIS_CAT)
    def test_false_raises_when_missing(self, MockCat):
        """create_if_missing=False raises TableNotFoundError instead of
        bootstrapping."""
        from supertable.simple_table import SimpleTable

        cat = MagicMock()
        cat.leaf_exists.return_value = False
        MockCat.return_value = cat

        parent = _mock_super_table(org="ghost_org", sup="ghost_sup")

        with pytest.raises(TableNotFoundError) as exc_info:
            SimpleTable(parent, "ghost_table", create_if_missing=False)

        # Error carries the coordinates
        assert exc_info.value.organization == "ghost_org"
        assert exc_info.value.super_name == "ghost_sup"
        assert exc_info.value.simple_name == "ghost_table"
        assert "ghost_org/ghost_sup/ghost_table" in str(exc_info.value)

        # And no side effects happened
        parent.storage.makedirs.assert_not_called()
        parent.storage.write_json.assert_not_called()
        cat.set_leaf_payload_cas.assert_not_called()
        cat.set_leaf_path_cas.assert_not_called()

    @patch(_P_SIMPLE_REDIS_CAT)
    def test_false_happy_path_when_leaf_exists(self, MockCat):
        """When the simple table exists, create_if_missing=False is a
        no-op — same fast path as the default."""
        from supertable.simple_table import SimpleTable

        cat = MagicMock()
        cat.leaf_exists.return_value = True
        MockCat.return_value = cat

        parent = _mock_super_table()
        st = SimpleTable(parent, "exists_table", create_if_missing=False)

        assert st.simple_name == "exists_table"
        cat.leaf_exists.assert_called_once()
        # Fast path: no mkdirs, no snapshot write
        parent.storage.makedirs.assert_not_called()
        parent.storage.write_json.assert_not_called()
