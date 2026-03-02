"""
Comprehensive test suite for supertable/super_table.py

Covers:
  1. SuperTable.__init__
     - Fast path: root exists in Redis → skip storage mkdirs, skip RBAC init
     - Slow path: root absent → makedirs, ensure_root, RBAC scaffolding
     - makedirs exception swallowed
     - Attributes wired correctly
  2. SuperTable.init_super_table
     - makedirs called with correct super_dir
     - makedirs exception swallowed gracefully
     - ensure_root called with org + super
  3. SuperTable.read_simple_table_snapshot
     - Happy path: file exists, size > 0 → returns parsed JSON
     - Empty path → FileNotFoundError
     - Path does not exist → FileNotFoundError
     - File exists but size == 0 → ValueError
     - storage.read_json exception propagates
  4. SuperTable.delete
     - Happy path: storage exists → delete storage + delete Redis keys
     - Storage does not exist → still deletes Redis keys
     - Storage delete raises FileNotFoundError → still deletes Redis keys
     - Storage delete raises other exception → propagates, Redis NOT deleted
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest


# ---------------------------------------------------------------------------
# Patch targets
# ---------------------------------------------------------------------------
_MOD = "supertable.super_table"
_P_GET_STORAGE = f"{_MOD}.get_storage"
_P_REDIS_CAT = f"{_MOD}.RedisCatalog"
_P_ROLE_MGR = f"{_MOD}.RoleManager"
_P_USER_MGR = f"{_MOD}.UserManager"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_super(
    super_name: str = "sup",
    organization: str = "org",
    root_exists: bool = True,
    storage: MagicMock | None = None,
    catalog: MagicMock | None = None,
):
    """Build a SuperTable via __new__ with mocked internals (skips __init__)."""
    from supertable.super_table import SuperTable

    st = SuperTable.__new__(SuperTable)
    st.identity = "super"
    st.super_name = super_name
    st.organization = organization
    st.storage = storage or MagicMock()
    st.catalog = catalog or MagicMock()
    st.super_dir = f"{organization}/{super_name}/super"
    return st


# ===========================================================================
# 1. SuperTable.__init__
# ===========================================================================

class TestSuperTableInit:

    @patch(_P_USER_MGR)
    @patch(_P_ROLE_MGR)
    @patch(_P_REDIS_CAT)
    @patch(_P_GET_STORAGE)
    def test_fast_path_root_exists_skips_init(
        self, mock_storage, MockCat, MockRole, MockUser,
    ):
        """When root exists in Redis, skip makedirs and RBAC init."""
        from supertable.super_table import SuperTable

        mock_stor = MagicMock()
        mock_storage.return_value = mock_stor
        mock_cat = MagicMock()
        mock_cat.root_exists.return_value = True
        MockCat.return_value = mock_cat

        st = SuperTable("my_super", "my_org")

        assert st.super_name == "my_super"
        assert st.organization == "my_org"
        assert st.storage is mock_stor
        assert st.catalog is mock_cat
        mock_cat.root_exists.assert_called_once_with("my_org", "my_super")
        # Should NOT call init_super_table or RBAC
        mock_stor.makedirs.assert_not_called()
        mock_cat.ensure_root.assert_not_called()
        MockRole.assert_not_called()
        MockUser.assert_not_called()

    @patch(_P_USER_MGR)
    @patch(_P_ROLE_MGR)
    @patch(_P_REDIS_CAT)
    @patch(_P_GET_STORAGE)
    def test_slow_path_root_absent_initializes(
        self, mock_storage, MockCat, MockRole, MockUser,
    ):
        """When root absent, calls makedirs, ensure_root, and RBAC scaffolding."""
        from supertable.super_table import SuperTable

        mock_stor = MagicMock()
        mock_storage.return_value = mock_stor
        mock_cat = MagicMock()
        mock_cat.root_exists.return_value = False
        MockCat.return_value = mock_cat

        st = SuperTable("sup", "org")

        mock_stor.makedirs.assert_called_once_with("org/sup/super")
        mock_cat.ensure_root.assert_called_once_with("org", "sup")
        MockRole.assert_called_once_with(super_name="sup", organization="org")
        MockUser.assert_called_once_with(super_name="sup", organization="org")

    @patch(_P_USER_MGR)
    @patch(_P_ROLE_MGR)
    @patch(_P_REDIS_CAT)
    @patch(_P_GET_STORAGE)
    def test_slow_path_makedirs_exception_swallowed(
        self, mock_storage, MockCat, MockRole, MockUser,
    ):
        """makedirs failure does not prevent rest of init."""
        from supertable.super_table import SuperTable

        mock_stor = MagicMock()
        mock_stor.makedirs.side_effect = OSError("no-op")
        mock_storage.return_value = mock_stor
        mock_cat = MagicMock()
        mock_cat.root_exists.return_value = False
        MockCat.return_value = mock_cat

        st = SuperTable("sup", "org")

        # ensure_root and RBAC should still be called
        mock_cat.ensure_root.assert_called_once()
        MockRole.assert_called_once()
        MockUser.assert_called_once()

    @patch(_P_USER_MGR)
    @patch(_P_ROLE_MGR)
    @patch(_P_REDIS_CAT)
    @patch(_P_GET_STORAGE)
    def test_super_dir_constructed_correctly(
        self, mock_storage, MockCat, MockRole, MockUser,
    ):
        from supertable.super_table import SuperTable

        mock_storage.return_value = MagicMock()
        mock_cat = MagicMock()
        mock_cat.root_exists.return_value = True
        MockCat.return_value = mock_cat

        st = SuperTable("my_super", "my_org")
        assert st.super_dir == "my_org/my_super/super"

    @patch(_P_USER_MGR)
    @patch(_P_ROLE_MGR)
    @patch(_P_REDIS_CAT)
    @patch(_P_GET_STORAGE)
    def test_identity_is_super(
        self, mock_storage, MockCat, MockRole, MockUser,
    ):
        from supertable.super_table import SuperTable

        mock_storage.return_value = MagicMock()
        MockCat.return_value = MagicMock(root_exists=MagicMock(return_value=True))

        st = SuperTable("s", "o")
        assert st.identity == "super"


# ===========================================================================
# 2. SuperTable.init_super_table
# ===========================================================================

class TestInitSuperTable:

    def test_calls_makedirs_and_ensure_root(self):
        st = _make_super("sup", "org")

        st.init_super_table()

        st.storage.makedirs.assert_called_once_with("org/sup/super")
        st.catalog.ensure_root.assert_called_once_with("org", "sup")

    def test_makedirs_exception_swallowed(self):
        st = _make_super()
        st.storage.makedirs.side_effect = OSError("object storage no-op")

        # Should not raise
        st.init_super_table()

        # ensure_root still called
        st.catalog.ensure_root.assert_called_once()

    def test_makedirs_any_exception_swallowed(self):
        st = _make_super()
        st.storage.makedirs.side_effect = RuntimeError("unexpected")

        st.init_super_table()
        st.catalog.ensure_root.assert_called_once()

    def test_ensure_root_exception_propagates(self):
        st = _make_super()
        st.catalog.ensure_root.side_effect = ConnectionError("redis down")

        with pytest.raises(ConnectionError, match="redis down"):
            st.init_super_table()


# ===========================================================================
# 3. SuperTable.read_simple_table_snapshot
# ===========================================================================

class TestReadSimpleTableSnapshot:

    def test_happy_path(self):
        st = _make_super()
        st.storage.exists.return_value = True
        st.storage.size.return_value = 1024
        expected = {"resources": [{"file": "f1"}], "schema": {"id": "int"}}
        st.storage.read_json.return_value = expected

        result = st.read_simple_table_snapshot("/path/to/snap.json")

        st.storage.exists.assert_called_once_with("/path/to/snap.json")
        st.storage.size.assert_called_once_with("/path/to/snap.json")
        st.storage.read_json.assert_called_once_with("/path/to/snap.json")
        assert result == expected

    def test_empty_path_raises_file_not_found(self):
        st = _make_super()

        with pytest.raises(FileNotFoundError, match="not found"):
            st.read_simple_table_snapshot("")

    def test_none_path_raises_file_not_found(self):
        st = _make_super()

        with pytest.raises(FileNotFoundError, match="not found"):
            st.read_simple_table_snapshot(None)

    def test_path_does_not_exist_raises_file_not_found(self):
        st = _make_super()
        st.storage.exists.return_value = False

        with pytest.raises(FileNotFoundError, match="not found"):
            st.read_simple_table_snapshot("/missing/snap.json")

    def test_size_zero_raises_value_error(self):
        st = _make_super()
        st.storage.exists.return_value = True
        st.storage.size.return_value = 0

        with pytest.raises(ValueError, match="empty"):
            st.read_simple_table_snapshot("/path/snap.json")

    def test_read_json_exception_propagates(self):
        st = _make_super()
        st.storage.exists.return_value = True
        st.storage.size.return_value = 100
        st.storage.read_json.side_effect = IOError("read failed")

        with pytest.raises(IOError, match="read failed"):
            st.read_simple_table_snapshot("/path/snap.json")

    def test_size_one_is_valid(self):
        st = _make_super()
        st.storage.exists.return_value = True
        st.storage.size.return_value = 1
        st.storage.read_json.return_value = {}

        result = st.read_simple_table_snapshot("/path/snap.json")
        assert result == {}

    def test_exists_not_called_when_path_empty(self):
        st = _make_super()
        with pytest.raises(FileNotFoundError):
            st.read_simple_table_snapshot("")
        st.storage.exists.assert_not_called()


# ===========================================================================
# 4. SuperTable.delete
# ===========================================================================

class TestDelete:

    def test_happy_path_deletes_storage_and_redis(self):
        st = _make_super("sup", "org")
        st.storage.exists.return_value = True

        st.delete(role_name="admin")

        st.storage.exists.assert_called_once_with("org/sup")
        st.storage.delete.assert_called_once_with("org/sup")
        st.catalog.delete_super_table.assert_called_once_with("org", "sup")

    def test_storage_not_exists_still_deletes_redis(self):
        st = _make_super("sup", "org")
        st.storage.exists.return_value = False

        st.delete(role_name="admin")

        st.storage.delete.assert_not_called()
        st.catalog.delete_super_table.assert_called_once_with("org", "sup")

    def test_storage_delete_file_not_found_still_deletes_redis(self):
        st = _make_super("sup", "org")
        st.storage.exists.return_value = True
        st.storage.delete.side_effect = FileNotFoundError("gone")

        st.delete(role_name="admin")

        st.catalog.delete_super_table.assert_called_once_with("org", "sup")

    def test_storage_delete_other_exception_propagates(self):
        st = _make_super("sup", "org")
        st.storage.exists.return_value = True
        st.storage.delete.side_effect = PermissionError("forbidden")

        with pytest.raises(PermissionError, match="forbidden"):
            st.delete(role_name="admin")

        # Redis should NOT be deleted when storage delete fails with non-FileNotFoundError
        st.catalog.delete_super_table.assert_not_called()

    def test_delete_uses_correct_base_dir(self):
        """base_dir = org/super_name (no /super suffix)."""
        st = _make_super("my_sup", "my_org")
        st.storage.exists.return_value = True

        st.delete(role_name="admin")

        st.storage.exists.assert_called_once_with("my_org/my_sup")
        st.storage.delete.assert_called_once_with("my_org/my_sup")

    def test_storage_exists_exception_propagates(self):
        st = _make_super()
        st.storage.exists.side_effect = ConnectionError("storage down")

        with pytest.raises(ConnectionError, match="storage down"):
            st.delete(role_name="admin")

        st.catalog.delete_super_table.assert_not_called()

    def test_redis_delete_exception_propagates(self):
        st = _make_super()
        st.storage.exists.return_value = False
        st.catalog.delete_super_table.side_effect = ConnectionError("redis down")

        with pytest.raises(ConnectionError, match="redis down"):
            st.delete(role_name="admin")

    def test_role_name_parameter_accepted(self):
        """delete() accepts role_name param (used by caller for RBAC, not enforced here)."""
        st = _make_super()
        st.storage.exists.return_value = False

        # Should not raise
        st.delete(role_name="some_role")
        st.catalog.delete_super_table.assert_called_once()
