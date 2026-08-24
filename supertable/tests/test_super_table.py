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

from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pytest

from supertable.rbac.permissions import Permission, RoleType


# ---------------------------------------------------------------------------
# Patch targets
# ---------------------------------------------------------------------------
_MOD = "supertable.super_table"
_P_GET_STORAGE = f"{_MOD}.get_storage"
_P_REDIS_CAT = f"{_MOD}.RedisCatalog"
_P_ROLE_MGR = f"{_MOD}.RoleManager"
_P_USER_MGR = f"{_MOD}.UserManager"
_P_RESOLVE_ROLE_CONTEXT = f"{_MOD}.resolve_role_access_context"


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
    st.catalog.root_exists.return_value = root_exists
    st.catalog.acquire_namespace_lock.return_value = "namespace-token"
    st.catalog.begin_namespace_deletion.return_value = {
        "intent_id": "namespace-delete-intent",
    }
    st.catalog.find_clones_strict.return_value = []
    st.catalog.scan_leaf_keys.return_value = iter(())
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
        mock_cat.acquire_namespace_lock.return_value = "namespace-token"
        MockCat.return_value = mock_cat

        st = SuperTable("sup", "org")

        mock_stor.makedirs.assert_called_once_with("org/sup/super")
        mock_cat.ensure_root.assert_called_once_with(
            "org", "sup", namespace_token="namespace-token",
        )
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
        mock_cat.acquire_namespace_lock.return_value = "namespace-token"
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
        st = _make_super("sup", "org", root_exists=False)

        st.init_super_table()

        st.storage.makedirs.assert_called_once_with("org/sup/super")
        st.catalog.ensure_root.assert_called_once_with(
            "org", "sup", namespace_token="namespace-token",
        )

    def test_makedirs_exception_swallowed(self):
        st = _make_super(root_exists=False)
        st.storage.makedirs.side_effect = OSError("object storage no-op")

        # Should not raise
        st.init_super_table()

        # ensure_root still called
        st.catalog.ensure_root.assert_called_once()

    def test_makedirs_any_exception_swallowed(self):
        st = _make_super(root_exists=False)
        st.storage.makedirs.side_effect = RuntimeError("unexpected")

        st.init_super_table()
        st.catalog.ensure_root.assert_called_once()

    def test_ensure_root_exception_propagates(self):
        st = _make_super(root_exists=False)
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

    @pytest.fixture(autouse=True)
    def _superadmin_context(self):
        """Patch the symbol imported by super_table; never consult real Redis."""
        with patch(_P_RESOLVE_ROLE_CONTEXT) as resolver:
            resolver.return_value = SimpleNamespace(
                role_type=RoleType.SUPERADMIN,
            )
            self.resolve_role_context = resolver
            yield

    def test_happy_path_deletes_storage_and_redis(self):
        st = _make_super("sup", "org")

        st.delete(role_name="admin")

        st.storage.delete_prefix.assert_called_once_with("org/sup")
        st.catalog.delete_super_table.assert_called_once_with(
            "org", "sup", namespace_token="namespace-token",
            intent_id="namespace-delete-intent",
        )
        st.catalog.release_namespace_lock.assert_called_once_with(
            "org", "sup", "namespace-token",
        )
        self.resolve_role_context.assert_called_once_with(
            super_name="sup",
            organization="org",
            role_name="admin",
            permission=Permission.CONTROL,
            label="delete this SuperTable",
        )

    def test_cleanup_callback_runs_after_commit_while_fence_is_held(self):
        st = _make_super("sup", "org")
        events = []
        st.storage.delete_prefix.side_effect = lambda _path: events.append("storage")
        st.catalog.delete_super_table.side_effect = (
            lambda *_args, **_kwargs: events.append("catalog-commit")
        )
        st.catalog.release_namespace_lock.side_effect = (
            lambda *_args, **_kwargs: events.append("fence-release")
        )

        result = st.delete(
            role_name="admin",
            post_delete_cleanup_callback=lambda: events.append("platform-cleanup"),
        )

        assert result == "namespace-delete-intent"
        assert events == [
            "storage",
            "catalog-commit",
            "platform-cleanup",
            "fence-release",
        ]

    def test_cleanup_failure_is_explicit_post_commit_outcome(self):
        from supertable.super_table import NamespaceCleanupPostCommitError

        st = _make_super("sup", "org")

        def fail_cleanup():
            raise ConnectionError("redis://secret@internal")

        with pytest.raises(NamespaceCleanupPostCommitError) as raised:
            st.delete(
                role_name="admin",
                post_delete_cleanup_callback=fail_cleanup,
            )

        assert raised.value.core_committed is True
        assert raised.value.core_result == "namespace-delete-intent"
        assert raised.value.intent_id == "namespace-delete-intent"
        assert "secret" not in str(raised.value)
        st.storage.delete_prefix.assert_called_once_with("org/sup")
        st.catalog.delete_super_table.assert_called_once()
        st.catalog.release_namespace_lock.assert_called_once_with(
            "org", "sup", "namespace-token",
        )

    def test_recovery_tombstone_cleanup_failure_is_post_commit(self):
        from supertable.super_table import NamespaceCleanupPostCommitError

        st = _make_super("sup", "org")
        st.catalog.recover_namespace_deletion.return_value = {
            "intent_id": "recovered-intent",
        }
        st.catalog.clear_namespace_deletion_tombstone.side_effect = (
            ConnectionError("redis unavailable")
        )

        with pytest.raises(NamespaceCleanupPostCommitError) as raised:
            st.recover_delete(
                "admin",
                intent_id="recovered-intent",
                confirm_previous_owner_stopped=True,
            )

        assert raised.value.core_committed is True
        assert raised.value.intent_id == "recovered-intent"
        st.storage.delete_prefix.assert_called_once_with("org/sup")
        st.catalog.delete_super_table.assert_called_once()
        st.catalog.release_namespace_lock.assert_called_once_with(
            "org", "sup", "namespace-token",
        )

    def test_authorization_is_refreshed_immediately_before_storage_delete(self):
        st = _make_super("sup", "org")
        refreshed = iter(("admin", "revoked"))

        def authorize():
            return next(refreshed)

        self.resolve_role_context.side_effect = (
            SimpleNamespace(role_type=RoleType.SUPERADMIN),
            SimpleNamespace(role_type=RoleType.SUPERADMIN),
            SimpleNamespace(role_type=RoleType.ADMIN),
        )

        with pytest.raises(PermissionError, match="Only SUPERADMIN"):
            st.delete(role_name="admin", authorization_callback=authorize)

        assert self.resolve_role_context.call_count == 3
        st.storage.delete_prefix.assert_not_called()
        st.catalog.delete_super_table.assert_not_called()
        st.catalog.release_namespace_lock.assert_called_once_with(
            "org", "sup", "namespace-token",
        )

    def test_empty_prefix_still_deletes_redis(self):
        st = _make_super("sup", "org")

        st.delete(role_name="admin")

        st.storage.delete_prefix.assert_called_once_with("org/sup")
        st.catalog.delete_super_table.assert_called_once_with(
            "org", "sup", namespace_token="namespace-token",
            intent_id="namespace-delete-intent",
        )

    def test_storage_prefix_file_not_found_keeps_redis(self):
        st = _make_super("sup", "org")
        st.storage.delete_prefix.side_effect = FileNotFoundError("gone")

        with pytest.raises(FileNotFoundError, match="gone"):
            st.delete(role_name="admin")

        st.catalog.delete_super_table.assert_not_called()

    def test_storage_delete_other_exception_propagates(self):
        st = _make_super("sup", "org")
        st.storage.delete_prefix.side_effect = PermissionError("forbidden")

        with pytest.raises(PermissionError, match="forbidden"):
            st.delete(role_name="admin")

        # Redis should NOT be deleted when storage delete fails with non-FileNotFoundError
        st.catalog.delete_super_table.assert_not_called()

    def test_delete_uses_correct_base_dir(self):
        """base_dir = org/super_name (no /super suffix)."""
        st = _make_super("my_sup", "my_org")

        st.delete(role_name="admin")

        st.storage.delete_prefix.assert_called_once_with("my_org/my_sup")

    def test_storage_prefix_exception_propagates(self):
        st = _make_super()
        st.storage.delete_prefix.side_effect = ConnectionError("storage down")

        with pytest.raises(ConnectionError, match="storage down"):
            st.delete(role_name="admin")

        st.catalog.delete_super_table.assert_not_called()

    def test_redis_delete_exception_propagates(self):
        st = _make_super()
        st.catalog.delete_super_table.side_effect = ConnectionError("redis down")

        with pytest.raises(ConnectionError, match="redis down"):
            st.delete(role_name="admin")

    def test_non_superadmin_is_denied_before_mutation(self):
        """ADMIN has CONTROL generally but cannot delete a whole namespace."""
        st = _make_super()
        self.resolve_role_context.return_value = SimpleNamespace(
            role_type=RoleType.ADMIN,
        )

        with pytest.raises(PermissionError, match="Only SUPERADMIN"):
            st.delete(role_name="admin")

        st.storage.delete_prefix.assert_not_called()
        st.catalog.delete_super_table.assert_not_called()
