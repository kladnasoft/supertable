"""
Tests for ``DataWriter.compact()`` — the explicit lock-protected
compaction entry point.

These tests mock the heavy dependencies (RedisCatalog, SuperTable,
SimpleTable, processing helpers) so they exercise the orchestration
logic in ``DataWriter.compact()`` itself: the lock lifecycle, snapshot
read, tombstone-compaction gate, small-file compaction, snapshot
commit, GC enqueue, monitoring, and audit.

End-to-end value-preservation properties (no row loss / dup / column
drift) are covered by ``test_processing_compact_resources.py`` against
real Parquet I/O.
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, call, patch

import polars as pl
import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")


# ---------------------------------------------------------------------------
# Patch targets
# ---------------------------------------------------------------------------

_MOD = "supertable.data_writer"
_P_CHECK_WRITE   = f"{_MOD}.check_write_access"
_P_SUPER_TABLE   = f"{_MOD}.SuperTable"
_P_SIMPLE_TABLE  = f"{_MOD}.SimpleTable"
_P_REDIS_CAT     = f"{_MOD}.RedisCatalog"
_P_COMPACT_RES   = f"{_MOD}.compact_resources"
_P_COMPACT_TOMB  = f"{_MOD}.compact_tombstones"
_P_READ_PARQUET  = f"{_MOD}._read_parquet_safe"
_P_LOAD_TOMB     = f"{_MOD}.load_tombstone"
_P_PERSIST_V2    = f"{_MOD}.persist_tombstone_v2_frame"
_P_BUILD_STATS   = f"{_MOD}.build_stats_file"
_P_MIRROR        = f"{_MOD}.MirrorFormats"
_P_MON_WRITER    = f"{_MOD}.MonitoringWriter"
_P_AUDIT         = f"{_MOD}._audit_emit"
_P_SETTINGS      = f"{_MOD}.settings"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resource(file: str, file_size: int = 1_000, rows: int = 100) -> dict:
    return {
        "file": file,
        "file_size": file_size,
        "rows": rows,
        "columns": [{"name": "id", "type": "Int64"}],
        "stats": None,
    }


def _snapshot(
    resources: list, *, tombstone: str | None = None, tombstone_rows: int = 1,
) -> dict:
    snap = {
        "simple_name": "orders",
        "snapshot_version": 1,
        "resources": resources,
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    if tombstone is not None:
        # In the merge-on-read model the snapshot stores a POINTER (path)
        # to the deletion-vector parquet, not an inline dict. compact()
        # reads it via ``_read_parquet_safe`` and gates draining purely
        # on whether that frame has rows (tombstone_rows > 0).
        snap["tombstone"] = tombstone
        frame = _dv_frame(
            tombstone_rows, file=resources[0]["file"] if resources else "a"
        )
        from supertable.processing import tombstone_digest
        snap["tombstone_rows"] = frame.height
        snap["tombstone_digest"] = tombstone_digest(frame)
    return snap


def _dv_frame(n_rows: int, file: str = "a"):
    """A stand-in deletion-vector frame whose ``.height`` is ``n_rows``."""
    return pl.DataFrame(
        {"__file__": [file] * n_rows, "__rowid__": list(range(1, n_rows + 1))},
        schema={"__file__": pl.Utf8, "__rowid__": pl.Int64},
    )


def _mk_simple_mock(snap: dict, *, snap_path: str = "/snap/v1.json",
                    data_dir: str = "/d"):
    """Build a SimpleTable mock returning the given snapshot."""
    mock = MagicMock()
    mock.simple_dir = "acme/warehouse/tables/tbl"
    mock.data_dir = data_dir
    mock._last_snapshot_leaf = {"version": 1, "path": snap_path}
    mock.get_simple_table_snapshot.return_value = (snap, snap_path)
    # ``update`` returns (new_snapshot_dict, new_snapshot_path).
    # Callers patch this further if they care.
    default_snapshot = {
        **snap,
        "resources": [{"file": "compacted.parquet"}],
        "snapshot_version": 2,
    }
    mock.update.return_value = (default_snapshot, "/snap/v2.json")

    def update(
            new_resources, sunset_files, model_df,
            *, last_snapshot, **_kwargs,
    ):
        updated = dict(last_snapshot)
        updated["resources"] = [{"file": "compacted.parquet"}]
        updated["snapshot_version"] = (
            int(last_snapshot.get("snapshot_version", 1)) + 1
        )
        return updated, "/snap/v2.json"

    mock.update.side_effect = update
    return mock


def _stub_settings():
    """Build a fake ``settings`` object for patching."""
    return MagicMock()


@pytest.fixture(autouse=True)
def _stub_compaction_stats_io():
    """Keep this orchestration suite isolated from footer/stat storage I/O.

    Exact footer caching and statistics persistence are exercised by their
    dedicated processing tests; these tests use synthetic resource paths.
    """
    with (
        patch(f"{_MOD}.extract_stats_rows", return_value=pl.DataFrame()),
        patch(_P_BUILD_STATS, return_value=(None, None)),
    ):
        yield


def _build_writer():
    """Construct a DataWriter via __new__ — skips the real __init__
    (which would touch SuperTable + RedisCatalog)."""
    from supertable.data_writer import DataWriter
    dw = DataWriter.__new__(DataWriter)
    dw.super_table = MagicMock(super_name="warehouse", organization="acme")
    dw.super_table.storage = MagicMock()
    class AtomicCatalogDouble:
        def __init__(self):
            self.acquire_simple_lock = MagicMock(return_value="tok")
            self.release_simple_lock = MagicMock(return_value=True)
            self.commit_snapshot_mock = MagicMock(return_value=(2, 2))
            self.set_leaf_payload_cas = MagicMock()
            self.bump_root = MagicMock()
            self.root_exists = MagicMock()
            self.leaf_exists = MagicMock()
            self._mirrors = []
            self.mirror_state_events = []

        def commit_snapshot(self, *args, **kwargs):
            result = self.commit_snapshot_mock(*args, **kwargs)
            if kwargs.get("mirror_publication"):
                self.mirror_state_events.append("core_committed")
            return result

        def get_mirrors(self, organization, super_name):
            return list(self._mirrors)

        def prepare_mirror_publication(self, *args, **kwargs):
            self.mirror_state_events.append("prepared")
            return {"status": "prepared"}

        def complete_mirror_publication(self, *args, **kwargs):
            self.mirror_state_events.append("complete")
            return {"status": "complete"}

        def fail_mirror_publication(self, *args, **kwargs):
            self.mirror_state_events.append("failed")
            return {"status": "failed"}

    dw.catalog = AtomicCatalogDouble()
    dw.catalog.acquire_simple_lock.return_value = "tok"
    dw.catalog.release_simple_lock.return_value = True
    dw._table_config_cache = {}
    return dw


# ===========================================================================
# 1. Access control + lock lifecycle
# ===========================================================================


class TestAccessAndLock:

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES, return_value=(0, 0, [], set()))
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_calls_check_write_access(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        dw = _build_writer()
        MockSimple.return_value = _mk_simple_mock(_snapshot([]))
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl")

        expected = call(
            super_name="warehouse", organization="acme",
            role_name="admin", table_name="tbl",
        )
        assert mock_check_write.call_args_list == [expected, expected]

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_authorization_callback_fences_lock_and_publication(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        dw = _build_writer()
        events: list[str] = []
        roles = iter(("preflight", "locked", "publisher"))

        def authorize() -> str:
            role = next(roles)
            events.append(f"auth:{role}")
            return role

        mock_check_write.side_effect = lambda **kwargs: events.append(
            f"check:{kwargs['role_name']}"
        )
        dw.catalog.acquire_simple_lock.side_effect = lambda *_a, **_k: (
            events.append("lock") or "tok"
        )

        snap = _snapshot([_resource("a"), _resource("b")])
        mock_simple = _mk_simple_mock(snap)
        MockSimple.side_effect = lambda *_a, **_k: (
            events.append("simple") or mock_simple
        )
        mock_simple.get_simple_table_snapshot.side_effect = lambda: (
            events.append("snapshot") or (snap, "/snap/v1.json")
        )
        new_resource = {
            "file": "c.parquet", "file_size": 5_000,
            "columns": [{"name": "id"}],
        }
        mock_compact_res.side_effect = lambda **_kwargs: (
            events.append("rewrite") or
            (2, 200, [new_resource], {"a", "b"})
        )

        def update(*_args, **kwargs):
            events.append(f"lineage:{kwargs['lineage']['role_name']}")
            return (
                {**snap, "resources": [new_resource], "snapshot_version": 2},
                "/snap/v2.json",
            )

        mock_simple.update.side_effect = update
        dw.catalog.commit_snapshot_mock.side_effect = lambda *_a, **_k: (
            events.append("publish") or (2, 2)
        )
        dw._get_table_config = MagicMock(return_value={})

        result = dw.compact(
            "stale", "tbl", authorization_callback=authorize,
        )

        assert events == [
            "auth:preflight",
            "check:preflight",
            "lock",
            "auth:locked",
            "check:locked",
            "simple",
            "snapshot",
            "rewrite",
            "lineage:locked",
            "auth:publisher",
            "check:publisher",
            "publish",
        ]
        assert result["role_name"] == "publisher"
        assert dw.catalog.release_simple_lock.call_count == 1

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_authorization_revoked_after_lock_prevents_snapshot_read(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        dw = _build_writer()
        calls = 0

        def authorize() -> str:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise PermissionError("membership revoked")
            return "admin"

        with pytest.raises(PermissionError, match="membership revoked"):
            dw.compact("stale", "tbl", authorization_callback=authorize)

        assert calls == 2
        assert mock_check_write.call_count == 1
        MockSimple.assert_not_called()
        mock_compact_res.assert_not_called()
        dw.catalog.commit_snapshot_mock.assert_not_called()
        dw.catalog.release_simple_lock.assert_called_once()

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_authorization_revoked_before_publication_fails_closed(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        dw = _build_writer()
        calls = 0

        def authorize() -> str:
            nonlocal calls
            calls += 1
            if calls == 3:
                raise PermissionError("membership revoked")
            return "admin"

        snap = _snapshot([_resource("a"), _resource("b")])
        mock_simple = _mk_simple_mock(snap)
        MockSimple.return_value = mock_simple
        mock_compact_res.return_value = (
            2,
            200,
            [{
                "file": "c.parquet", "file_size": 5_000,
                "columns": [{"name": "id"}],
            }],
            {"a", "b"},
        )
        dw._get_table_config = MagicMock(return_value={})

        with pytest.raises(PermissionError, match="membership revoked"):
            dw.compact("stale", "tbl", authorization_callback=authorize)

        assert calls == 3
        assert mock_check_write.call_count == 2
        mock_simple.update.assert_called_once()
        dw.catalog.commit_snapshot_mock.assert_not_called()
        dw.catalog.set_leaf_payload_cas.assert_not_called()
        dw.catalog.bump_root.assert_not_called()
        dw.catalog.release_simple_lock.assert_called_once()

    @pytest.mark.parametrize("invalid_role", [None, "", "   ", 7])
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_authorization_callback_rejects_invalid_role(
        self, mock_check_write, MockSimple, invalid_role,
    ):
        dw = _build_writer()

        with pytest.raises(
            PermissionError, match="Compaction authorization is unavailable",
        ):
            dw.compact(
                "stale", "tbl",
                authorization_callback=lambda: invalid_role,
            )

        mock_check_write.assert_not_called()
        dw.catalog.acquire_simple_lock.assert_not_called()
        MockSimple.assert_not_called()

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES, return_value=(0, 0, [], set()))
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_acquires_and_releases_lock(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        dw = _build_writer()
        MockSimple.return_value = _mk_simple_mock(_snapshot([]))
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl")

        dw.catalog.acquire_simple_lock.assert_called_once_with(
            "acme", "warehouse", "tbl", ttl_s=30, timeout_s=60,
        )
        dw.catalog.release_simple_lock.assert_called_once()

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES, return_value=(0, 0, [], set()))
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_lock_acquisition_failure_raises_timeout(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        dw = _build_writer()
        dw.catalog.acquire_simple_lock.return_value = None  # lock denied
        MockSimple.return_value = _mk_simple_mock(_snapshot([]))

        with pytest.raises(TimeoutError, match="Could not acquire lock"):
            dw.compact("admin", "tbl")

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES, return_value=(0, 0, [], set()))
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_lock_released_even_when_processing_raises(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        dw = _build_writer()
        mock_compact_res.side_effect = RuntimeError("boom in compaction")
        MockSimple.return_value = _mk_simple_mock(_snapshot([_resource("a")]))
        dw._get_table_config = MagicMock(return_value={})

        with pytest.raises(RuntimeError, match="boom"):
            dw.compact("admin", "tbl")

        dw.catalog.release_simple_lock.assert_called_once()


# ===========================================================================
# 2. Read-only-on-missing — refuses to bootstrap
# ===========================================================================


class TestRefusesBootstrap:

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_simple_table_constructed_with_create_if_missing_false(
        self, mock_check_write, MockSimple,
    ):
        """compact() must pass create_if_missing=False to SimpleTable
        (defence in depth, in case the pre-flight is bypassed)."""
        dw = _build_writer()
        # Pre-flight needs to PASS so we reach the SimpleTable construction
        dw.catalog.root_exists.return_value = True
        dw.catalog.leaf_exists.return_value = True

        # Make SimpleTable raise to short-circuit the rest of compact()
        from supertable.errors import TableNotFoundError
        MockSimple.side_effect = TableNotFoundError("acme", "warehouse", "ghost")

        with pytest.raises(TableNotFoundError):
            dw.compact("admin", "ghost")

        # Verify the kwarg
        MockSimple.assert_called_once()
        args, kwargs = MockSimple.call_args
        assert kwargs.get("create_if_missing") is False, (
            f"compact must pass create_if_missing=False; got kwargs={kwargs}"
        )

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_missing_super_raises_before_rbac_bootstrap(
        self, mock_check_write, MockSimple,
    ):
        """Regression: ``check_write_access`` builds RoleManager which
        bootstraps RBAC role storage for missing supertables. The
        pre-flight existence check MUST run first so a compact() call
        against a ghost supertable raises ``SuperTableNotFoundError``
        without minting RBAC state."""
        dw = _build_writer()
        # Pre-flight: root missing
        dw.catalog.root_exists.return_value = False

        from supertable.errors import SuperTableNotFoundError
        with pytest.raises(SuperTableNotFoundError):
            dw.compact("admin", "ghost_table")

        # CRITICAL: check_write_access (which would bootstrap RBAC) MUST
        # NOT have been called when the pre-flight failed.
        mock_check_write.assert_not_called()
        # SimpleTable also not touched
        MockSimple.assert_not_called()
        # No lock acquired
        dw.catalog.acquire_simple_lock.assert_not_called()

    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_missing_leaf_raises_before_rbac_bootstrap(
        self, mock_check_write, MockSimple,
    ):
        """Regression: pre-flight must catch missing simple table
        before RBAC check too."""
        dw = _build_writer()
        # Pre-flight: super exists, leaf doesn't
        dw.catalog.root_exists.return_value = True
        dw.catalog.leaf_exists.return_value = False

        from supertable.errors import TableNotFoundError
        with pytest.raises(TableNotFoundError):
            dw.compact("admin", "ghost_table")

        mock_check_write.assert_not_called()
        MockSimple.assert_not_called()
        dw.catalog.acquire_simple_lock.assert_not_called()


# ===========================================================================
# 3. Tombstone compaction gating
# ===========================================================================


class TestTombstoneGating:
    """compact() drains the deletion-vector whenever it has rows.

    Post-refactor, draining is gated purely on ``tombstone_rows > 0``
    (the frame read from the snapshot's ``tombstone`` pointer). The
    ``force_tombstones`` flag is retained for API/lineage compatibility
    but no longer gates the DV-drain, and the per-table
    ``max_tombstone_rows`` threshold gates only the WRITE path.
    """

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(
        _P_COMPACT_RES,
        return_value=(
            0, 0, [], set(), _dv_frame(1),
        ),
    )
    @patch(_P_COMPACT_TOMB)
    @patch(_P_READ_PARQUET)
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_force_tombstones_true_drains_dv(
        self, mock_check_write, MockSimple, mock_settings, mock_read_pq,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """force_tombstones=True + a non-empty DV uses the fused drain."""
        dw = _build_writer()
        snap = _snapshot(
            [_resource("a"), _resource("b")],
            tombstone="/d/tombstone.parquet", tombstone_rows=1,
        )
        MockSimple.return_value = _mk_simple_mock(snap)
        mock_read_pq.return_value = _dv_frame(1)  # 1 tombstoned row
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl", force_tombstones=True)

        mock_compact_tomb.assert_not_called()
        assert mock_compact_res.call_args.kwargs["tombstone_df"].height == 1
        assert mock_compact_res.call_args.kwargs["return_residual"] is True

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(
        _P_COMPACT_RES,
        return_value=(
            0, 0, [], set(), _dv_frame(2),
        ),
    )
    @patch(_P_COMPACT_TOMB)
    @patch(_P_READ_PARQUET)
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_force_tombstones_false_still_drains_dv(
        self, mock_check_write, MockSimple, mock_settings, mock_read_pq,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """force_tombstones=False no longer suppresses the drain: a
        non-empty DV is still consumed unconditionally."""
        dw = _build_writer()
        snap = _snapshot(
            [_resource("a")],
            tombstone="/d/tombstone.parquet", tombstone_rows=2,
        )
        MockSimple.return_value = _mk_simple_mock(snap)
        mock_read_pq.return_value = _dv_frame(2)  # 2 tombstoned rows
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl", force_tombstones=False)

        mock_compact_tomb.assert_not_called()
        assert mock_compact_res.call_args.kwargs["tombstone_df"].height == 2

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(
        _P_COMPACT_RES,
        return_value=(
            0, 0, [], set(), _dv_frame(3),
        ),
    )
    @patch(_P_COMPACT_TOMB)
    @patch(_P_READ_PARQUET)
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_threshold_does_not_gate_compact_drain(
        self, mock_check_write, MockSimple, mock_settings, mock_read_pq,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """A tiny DV (well below any max_tombstone_rows threshold) is
        still drained — the threshold gates only the write path."""
        dw = _build_writer()
        snap = _snapshot(
            [_resource("a")],
            tombstone="/d/tombstone.parquet", tombstone_rows=3,
        )
        MockSimple.return_value = _mk_simple_mock(snap)
        mock_read_pq.return_value = _dv_frame(3)
        # A huge per-table threshold must NOT suppress compact()'s drain.
        dw._get_table_config = MagicMock(return_value={"max_tombstone_rows": 1_000_000})

        dw.compact("admin", "tbl", force_tombstones=False)

        mock_compact_tomb.assert_not_called()
        assert mock_compact_res.call_args.kwargs["tombstone_df"].height == 3

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES, return_value=(0, 0, [], set()))
    @patch(_P_COMPACT_TOMB)
    @patch(_P_READ_PARQUET)
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_empty_dv_skips_tombstone_step(
        self, mock_check_write, MockSimple, mock_settings, mock_read_pq,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """No tombstone rows → tombstone step is skipped.

        Two ways to have no rows: an empty DV frame, or no ``tombstone``
        pointer at all. Both must skip compact_tombstones."""
        dw = _build_writer()
        snap = _snapshot([_resource("a")])
        MockSimple.return_value = _mk_simple_mock(snap)
        mock_read_pq.return_value = _dv_frame(0)  # no pointer means no read
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl", force_tombstones=True)

        mock_compact_tomb.assert_not_called()

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES, return_value=(0, 0, [], set()))
    @patch(_P_COMPACT_TOMB)
    @patch(_P_READ_PARQUET)
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_no_tombstone_pointer_skips_tombstone_step(
        self, mock_check_write, MockSimple, mock_settings, mock_read_pq,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """A snapshot without a ``tombstone`` pointer never reads the DV
        and never runs the tombstone step."""
        dw = _build_writer()
        snap = _snapshot([_resource("a")])  # no tombstone pointer
        MockSimple.return_value = _mk_simple_mock(snap)
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl", force_tombstones=True)

        mock_compact_tomb.assert_not_called()
        mock_read_pq.assert_not_called()

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB)
    @patch(_P_READ_PARQUET)
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_v2_full_drain_preserves_marker_without_zero_manifest(
        self, mock_check_write, MockSimple, mock_settings, mock_read_pq,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        from supertable.processing import LoadedTombstoneState, tombstone_digest
        from supertable.tombstone_manifest_v2 import TombstoneSegment

        dw = _build_writer()
        manifest_path = (
            "acme/warehouse/tables/tbl/tombstone/manifest.json"
        )
        segment_path = (
            "acme/warehouse/tables/tbl/tombstone/segment.parquet"
        )
        frame = _dv_frame(1, file="a")
        segment = TombstoneSegment(
            file=segment_path,
            rows=1,
            file_size=123,
            digest=tombstone_digest(frame),
        )
        state = LoadedTombstoneState(
            frame=frame,
            tombstone_format=2,
            tombstone_path=manifest_path,
            root_digest="b" * 64,
            segments=(segment,),
        )
        snap = _snapshot([_resource("a")])
        snap.update({
            "tombstone": manifest_path,
            "tombstone_rows": 1,
            "tombstone_digest": state.root_digest,
            "tombstone_format": 2,
        })
        mock_simple = _mk_simple_mock(snap)
        mock_simple.simple_dir = "acme/warehouse/tables/tbl"
        MockSimple.return_value = mock_simple
        empty = _dv_frame(0)
        mock_compact_res.return_value = (1, 0, [], {"a"}, empty)
        dw._get_table_config = MagicMock(return_value={})

        def load_v2(_path, **kwargs):
            kwargs["state_out"]["state"] = state
            return frame

        with (
            patch(_P_LOAD_TOMB, side_effect=load_v2) as load_tomb,
            patch(_P_PERSIST_V2) as persist_v2,
        ):
            dw.compact("admin", "tbl", force_tombstones=True)

        load_tomb.assert_called_once()
        assert load_tomb.call_args.kwargs["tombstone_format"] == 2
        mock_read_pq.assert_not_called()
        persist_v2.assert_not_called()
        pinned = mock_simple.update.call_args.kwargs["last_snapshot"]
        assert pinned["tombstone_format"] == 2
        assert pinned["tombstone"] is None
        assert pinned["tombstone_rows"] == 0
        assert pinned["tombstone_digest"] is None


# ===========================================================================
# 4. Small-file compaction is delegated to processing.compact_resources
# ===========================================================================


class TestSmallFileDelegation:

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_compact_resources_called_with_small_only_default(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        dw = _build_writer()
        snap = _snapshot([_resource("a")])
        mock_simple = _mk_simple_mock(snap, data_dir="/data/orders")
        MockSimple.return_value = mock_simple
        mock_compact_res.return_value = (0, 0, [], set())
        dw._get_table_config = MagicMock(return_value={"max_memory_chunk_size": 1024})

        dw.compact("admin", "tbl")

        mock_compact_res.assert_called_once()
        kwargs = mock_compact_res.call_args.kwargs
        assert kwargs["data_dir"] == "/data/orders"
        assert kwargs["compression_level"] == 1
        assert kwargs["small_only"] is True
        assert kwargs["table_config"] == {"max_memory_chunk_size": 1024}

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_small_only_false_propagated(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        dw = _build_writer()
        snap = _snapshot([_resource("a")])
        MockSimple.return_value = _mk_simple_mock(snap)
        mock_compact_res.return_value = (0, 0, [], set())
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl", small_only=False)

        kwargs = mock_compact_res.call_args.kwargs
        assert kwargs["small_only"] is False


# ===========================================================================
# 5. Snapshot commit lifecycle
# ===========================================================================


class TestSnapshotCommit:

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_mirror_failure_reports_committed_compaction_and_releases_lock(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """Mirror failure is not success, but cannot roll back the core commit."""
        from supertable.mirroring.mirror_formats import MirrorPublicationError

        dw = _build_writer()
        dw.catalog._mirrors = ["PARQUET"]
        snap = _snapshot([_resource("a"), _resource("b")])
        mock_simple = _mk_simple_mock(snap)
        MockSimple.return_value = mock_simple
        mock_compact_res.return_value = (
            2,
            200,
            [{"file": "c.parquet", "file_size": 5000, "columns": [{"name": "id"}]}],
            {"a", "b"},
        )
        MockMirror.mirror_if_enabled.side_effect = OSError("mirror unavailable")
        dw._get_table_config = MagicMock(return_value={})

        with pytest.raises(MirrorPublicationError) as raised:
            dw.compact("admin", "tbl")

        error = raised.value
        assert error.core_committed is True
        assert error.snapshot_path == "/snap/v2.json"
        assert error.mirrors == ("PARQUET",)
        assert error.core_result["files_after"] == 1
        assert isinstance(error.__cause__, OSError)
        dw.catalog.commit_snapshot_mock.assert_called_once()
        dw.catalog.release_simple_lock.assert_called_once()
        mock_audit.assert_called_once()
        assert dw.catalog.mirror_state_events == [
            "prepared", "core_committed", "failed",
        ]

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_update_simpletable_called_with_aggregated_results(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        dw = _build_writer()
        snap = _snapshot([_resource("a"), _resource("b")])
        mock_simple = _mk_simple_mock(snap)
        MockSimple.return_value = mock_simple

        new_res = [{"file": "c.parquet", "file_size": 5000, "columns": [{"name": "id"}]}]
        sunset = {"a", "b"}
        mock_compact_res.return_value = (2, 200, new_res, sunset)
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl")

        mock_simple.update.assert_called_once()
        args, kwargs = mock_simple.update.call_args
        # First positional arg = new_resources, second = sunset_files
        assert args[0] == new_res
        assert args[1] == sunset

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES, return_value=(0, 0, [], set()))
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_short_circuit_when_nothing_to_compact(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """When compact_resources returns no new files and no sunsets,
        the snapshot must NOT be re-written — the result returns
        files_after == files_before."""
        dw = _build_writer()
        snap = _snapshot([_resource("a"), _resource("b")])
        mock_simple = _mk_simple_mock(snap)
        MockSimple.return_value = mock_simple
        dw._get_table_config = MagicMock(return_value={})

        result = dw.compact("admin", "tbl")

        # No snapshot update and no atomic catalog publication.
        mock_simple.update.assert_not_called()
        dw.catalog.commit_snapshot_mock.assert_not_called()
        # The result still has files_before / files_after equal
        assert result["files_before"] == 2
        assert result["files_after"] == 2

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES, return_value=(0, 0, [], set()))
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_short_circuit_still_emits_monitoring_and_audit(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """Regression: an early ``return`` inside the try block used to
        bypass the monitoring + audit emission code that lives after
        the try/finally. Even a no-op compaction is a compaction
        ATTEMPT and must be observable."""
        dw = _build_writer()
        snap = _snapshot([_resource("a")])
        mock_simple = _mk_simple_mock(snap)
        MockSimple.return_value = mock_simple
        mock_mon = MagicMock()
        MockMW.return_value.__enter__.return_value = mock_mon
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl")

        # Lock was released even on short-circuit
        dw.catalog.release_simple_lock.assert_called_once()
        # Monitoring metric was emitted (compact monitor type)
        MockMW.assert_called_once()
        assert MockMW.call_args.kwargs["monitor_type"] == "compact"
        mock_mon.log_metric.assert_called_once()
        # Audit event was emitted
        mock_audit.assert_called_once()

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_atomic_fenced_commit_after_successful_compaction(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        dw = _build_writer()
        snap = _snapshot([_resource("a"), _resource("b")])
        mock_simple = _mk_simple_mock(snap)
        MockSimple.return_value = mock_simple

        new_res = [{"file": "c.parquet", "file_size": 5000, "columns": [{"name": "id"}]}]
        mock_compact_res.return_value = (2, 200, new_res, {"a", "b"})
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl")

        dw.catalog.commit_snapshot_mock.assert_called_once()

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_durability_barrier_precedes_catalog_and_batch_exits(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        events = []

        class Batch:
            def __enter__(self):
                events.append("enter")
                return self

            def barrier(self):
                events.append("barrier")

            def catalog_commit_started(self):
                events.append("commit_started")

            def catalog_commit_succeeded(self):
                events.append("commit_succeeded")

            def __exit__(self, exc_type, exc_value, traceback):
                events.append(("exit", exc_type))
                return False

        dw = _build_writer()
        dw.super_table.storage.durability_batch.return_value = Batch()
        snap = _snapshot([_resource("a")])
        mock_simple = _mk_simple_mock(snap)
        update_result = mock_simple.update.return_value

        def update(*args, **kwargs):
            events.append("snapshot_written")
            return update_result

        mock_simple.update.side_effect = update
        MockSimple.return_value = mock_simple
        mock_compact_res.return_value = (
            1,
            100,
            [{"file": "c.parquet", "file_size": 1, "columns": []}],
            {"a"},
        )

        def commit(*args, **kwargs):
            events.append("redis")
            return (2, 2)

        dw.catalog.commit_snapshot_mock.side_effect = commit
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl")

        assert events == [
            "enter",
            "snapshot_written",
            "barrier",
            "commit_started",
            "redis",
            "commit_succeeded",
            ("exit", None),
        ]

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_durability_barrier_failure_exits_before_catalog(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        events = []

        class Batch:
            def __enter__(self):
                events.append("enter")
                return self

            def barrier(self):
                events.append("barrier")
                raise OSError("directory fsync failed")

            def __exit__(self, exc_type, exc_value, traceback):
                events.append(("exit", exc_type))
                return False

        dw = _build_writer()
        dw.super_table.storage.durability_batch.return_value = Batch()
        MockSimple.return_value = _mk_simple_mock(_snapshot([_resource("a")]))
        mock_compact_res.return_value = (
            1,
            100,
            [{"file": "c.parquet", "file_size": 1, "columns": []}],
            {"a"},
        )
        dw._get_table_config = MagicMock(return_value={})

        with pytest.raises(OSError, match="directory fsync failed"):
            dw.compact("admin", "tbl")

        assert events[0:2] == ["enter", "barrier"]
        assert events[-1] == ("exit", OSError)
        dw.catalog.commit_snapshot_mock.assert_not_called()
        dw.catalog.release_simple_lock.assert_called_once()


# ===========================================================================
# 7. Monitoring lifecycle
# ===========================================================================


class TestMonitoring:

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_monitoring_emits_compact_metric(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        dw = _build_writer()
        snap = _snapshot([_resource("a")])
        MockSimple.return_value = _mk_simple_mock(snap)
        mock_compact_res.return_value = (1, 100, [{"file": "c", "file_size": 1, "columns": []}], {"a"})
        mock_mon = MagicMock()
        MockMW.return_value.__enter__.return_value = mock_mon
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl")

        # Monitor constructed with monitor_type="compact"
        MockMW.assert_called_once()
        kwargs = MockMW.call_args.kwargs
        assert kwargs["monitor_type"] == "compact"
        assert kwargs["organization"] == "acme"

        # log_metric called once
        mock_mon.log_metric.assert_called_once()
        payload = mock_mon.log_metric.call_args.args[0]
        assert payload["supertables"] == ["warehouse"]
        assert payload["table_name"] == "tbl"

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_monitoring_spool_backpressure_is_explicit_after_compact_commit(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        from supertable.monitoring_writer import (
            MonitoringBackpressureError,
            MonitoringPostCommitError,
        )

        dw = _build_writer()
        MockSimple.return_value = _mk_simple_mock(_snapshot([_resource("a")]))
        mock_compact_res.return_value = (
            1, 100, [{"file": "c", "file_size": 1, "columns": []}], {"a"},
        )
        mock_mon = MagicMock()
        mock_mon.log_metric.side_effect = MonitoringBackpressureError("spool full")
        MockMW.return_value.__enter__.return_value = mock_mon
        dw._get_table_config = MagicMock(return_value={})

        with pytest.raises(MonitoringPostCommitError) as raised:
            dw.compact("admin", "tbl")

        assert raised.value.core_committed is True
        assert raised.value.operation == "compact"
        assert raised.value.core_result["files_after"] == 1

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_sink_table_compact_skips_monitoring(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """Compacting __compact__ itself must not emit a metric — same
        loop-guard semantics as write() against sink tables."""
        dw = _build_writer()
        snap = _snapshot([_resource("a")])
        MockSimple.return_value = _mk_simple_mock(snap)
        mock_compact_res.return_value = (1, 100, [{"file": "c", "file_size": 1, "columns": []}], {"a"})
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "__compact__")

        MockMW.assert_not_called()


# ===========================================================================
# 8. Schema preservation through update() — regression
# ===========================================================================
#
# compact() is a physical rewrite. Passing a model frame to update() would
# republish physical legacy columns or lose columns held by untouched files.
# These tests pin the ``model_df=None`` preservation boundary.


class TestSchemaPreservation:

    def test_footer_cache_contains_stats_only(self):
        """Physical compaction metadata must not override logical schema."""
        from supertable.processing import _FooterStatsCacheEntry

        entry = _FooterStatsCacheEntry(metadata=object(), rows=[])
        assert not hasattr(entry, "polars_schema")

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_explicit_compaction_preserves_schema_without_body_read(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """A physical rewrite must not replace last-write schema metadata."""
        dw = _build_writer()

        # Storage.read_parquet returns a real Arrow table with mixed dtypes
        import pyarrow as pa
        arrow_tbl = pa.table({
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["a", "b", "c"], type=pa.string()),
            "amount": pa.array([1.1, 2.2, 3.3], type=pa.float64()),
            "ok": pa.array([True, False, True], type=pa.bool_()),
        })
        dw.super_table.storage.read_parquet.return_value = arrow_tbl

        snap = _snapshot([_resource("a"), _resource("b")])
        mock_simple = _mk_simple_mock(snap)
        MockSimple.return_value = mock_simple

        new_res = [
            {"file": "/data/compacted.parquet", "file_size": 5000,
             "rows": 3, "columns": 4, "stats": None},
        ]
        mock_compact_res.return_value = (2, 3, new_res, {"a", "b"})
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl")

        args, kwargs = mock_simple.update.call_args
        assert args[2] is None
        dw.super_table.storage.read_parquet.assert_not_called()

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_explicit_compaction_preserves_prior_schema_when_storage_unavailable(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """Schema preservation needs no output read or reconstruction."""
        dw = _build_writer()

        # Storage.read_parquet raises — exercise the fallback
        dw.super_table.storage.read_parquet.side_effect = RuntimeError("io fail")

        # Snapshot with a real schema list (Spark-style entries)
        snap = {
            "simple_name": "orders",
            "snapshot_version": 1,
            "resources": [_resource("a"), _resource("b")],
            "schema": [
                {"name": "id", "type": "long", "nullable": True, "metadata": {}},
                {"name": "amount", "type": "double", "nullable": True, "metadata": {}},
                {"name": "flag", "type": "boolean", "nullable": True, "metadata": {}},
            ],
        }
        mock_simple = _mk_simple_mock(snap)
        MockSimple.return_value = mock_simple

        new_res = [{"file": "/data/compacted.parquet", "file_size": 5000,
                    "rows": 3, "columns": 3, "stats": None}]
        mock_compact_res.return_value = (2, 3, new_res, {"a", "b"})
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl")

        args, kwargs = mock_simple.update.call_args
        assert args[2] is None
        dw.super_table.storage.read_parquet.assert_not_called()

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_explicit_compaction_preserves_missing_schema(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """A missing prior schema stays missing instead of being guessed."""
        dw = _build_writer()
        dw.super_table.storage.read_parquet.side_effect = RuntimeError("nope")

        snap = {
            "simple_name": "orders",
            "snapshot_version": 1,
            "resources": [_resource("a")],
            # schema field absent
        }
        mock_simple = _mk_simple_mock(snap)
        MockSimple.return_value = mock_simple

        new_res = [{"file": "/data/compacted.parquet", "file_size": 5000,
                    "rows": 1, "columns": 1, "stats": None}]
        mock_compact_res.return_value = (1, 1, new_res, {"a"})
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl")

        args, kwargs = mock_simple.update.call_args
        assert args[2] is None
        dw.super_table.storage.read_parquet.assert_not_called()

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_explicit_compaction_does_not_derive_schema_from_output_chunks(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """Independent physical chunk schemas cannot redefine the table."""
        dw = _build_writer()

        import pyarrow as pa
        # Two new files with different schemas:
        # - chunk 1 has {id, name}
        # - chunk 2 has {id, name, email}  ← email only in chunk 2
        chunk1_arrow = pa.table({
            "id": pa.array([1], type=pa.int64()),
            "name": pa.array(["a"], type=pa.string()),
        })
        chunk2_arrow = pa.table({
            "id": pa.array([2], type=pa.int64()),
            "name": pa.array(["b"], type=pa.string()),
            "email": pa.array(["b@x"], type=pa.string()),
        })

        # Storage.read_parquet returns chunk1 for the first call, chunk2 for the second
        dw.super_table.storage.read_parquet.side_effect = [chunk1_arrow, chunk2_arrow]

        snap = _snapshot([_resource("a"), _resource("b"), _resource("c")])
        mock_simple = _mk_simple_mock(snap)
        MockSimple.return_value = mock_simple

        new_res = [
            {"file": "/data/chunk1.parquet", "file_size": 5000,
             "rows": 1, "columns": 2, "stats": None},
            {"file": "/data/chunk2.parquet", "file_size": 5000,
             "rows": 1, "columns": 3, "stats": None},
        ]
        mock_compact_res.return_value = (3, 2, new_res, {"a", "b", "c"})
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl")

        args, kwargs = mock_simple.update.call_args
        assert args[2] is None
        dw.super_table.storage.read_parquet.assert_not_called()


# ===========================================================================
# 9. Two-phase aggregation — tombstone output must not be re-sunset
# ===========================================================================


class TestTwoPhaseAggregation:
    """The fused replacement publishes only final resources, exactly once."""

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB)
    @patch(_P_READ_PARQUET)
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_tombstone_output_resunset_by_phase_b_not_in_new_resources(
        self, mock_check_write, MockSimple, mock_settings, mock_read_pq,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        dw = _build_writer()

        # Storage.read_parquet returns an arrow table for schema derivation
        import pyarrow as pa
        dw.super_table.storage.read_parquet.return_value = pa.table(
            {"id": pa.array([1], type=pa.int64())}
        )

        # Snapshot has A, B, C plus a non-empty deletion-vector.
        snap = _snapshot(
            [_resource("A", file_size=2_000_000),
             _resource("B", file_size=1_000_000),
             _resource("C", file_size=1_000_000)],
            tombstone="/d/tombstone.parquet", tombstone_rows=1,
        )
        mock_simple = _mk_simple_mock(snap)
        MockSimple.return_value = mock_simple
        mock_read_pq.return_value = _dv_frame(1, "A")  # DV has rows → drains

        # The fused pass consumes A's tombstone and packs A/B/C directly into
        # final G. There is no intermediate F to accidentally publish/sunset.
        G_resource = {
            "file": "G", "file_size": 3_500_000,
            "rows": 150, "columns": 1, "stats": None,
        }
        mock_compact_res.return_value = (
            3, 150, [G_resource], {"A", "B", "C"}, _dv_frame(0),
        )

        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl", force_tombstones=True)

        # Inspect the new_resources passed to simple_table.update.
        args, kwargs = mock_simple.update.call_args
        new_resources_passed = args[0]
        sunset_passed = args[1]

        new_files = {r["file"] for r in new_resources_passed}
        # Only the final output is published; no intermediate can leak in.
        assert new_files == {"G"}, (
            f"new_resources must exclude files in sunset; got new={new_files}, "
            f"sunset={sunset_passed}"
        )
        assert sunset_passed == {"A", "B", "C"}
        mock_compact_tomb.assert_not_called()

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB)
    @patch(_P_READ_PARQUET)
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_tombstone_output_survives_when_phase_b_does_not_sunset_it(
        self, mock_check_write, MockSimple, mock_settings, mock_read_pq,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """Multiple final fused outputs are each passed to update once."""
        dw = _build_writer()

        import pyarrow as pa
        dw.super_table.storage.read_parquet.return_value = pa.table(
            {"id": pa.array([1], type=pa.int64())}
        )

        snap = _snapshot(
            [_resource("A", file_size=2_000_000),
             _resource("B", file_size=500_000)],
            tombstone="/d/tombstone.parquet", tombstone_rows=1,
        )
        mock_simple = _mk_simple_mock(snap)
        MockSimple.return_value = mock_simple
        mock_read_pq.return_value = _dv_frame(1, "A")  # DV has rows → drains

        F_resource = {
            "file": "F", "file_size": 20_000_000,  # large
            "rows": 50, "columns": 1, "stats": None,
        }
        # A's survivor is large enough for its own final F; clean small B
        # becomes final G. Both are outputs of the one fused pass.
        G_resource = {
            "file": "G", "file_size": 500_000,
            "rows": 50, "columns": 1, "stats": None,
        }
        mock_compact_res.return_value = (
            2, 100, [F_resource, G_resource], {"A", "B"}, _dv_frame(0),
        )

        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl", force_tombstones=True)

        args, kwargs = mock_simple.update.call_args
        new_resources_passed = args[0]
        sunset_passed = args[1]
        new_files_list = [r["file"] for r in new_resources_passed]

        assert set(new_files_list) == {"F", "G"}, (
            f"update must receive each fused final file once; got {new_files_list}"
        )
        assert sunset_passed == {"A", "B"}, f"unexpected sunset={sunset_passed}"

        # The baseline contains no transient output; final F/G are introduced
        # only through new_resources.
        baseline_files = [r["file"] for r in kwargs["last_snapshot"]["resources"]]
        assert "F" not in baseline_files and "G" not in baseline_files

        # Effective snapshot = (baseline - sunset) + new_resources — F and G
        # each listed exactly once (a re-passed F would surface here as a dup).
        effective = [f for f in baseline_files if f not in sunset_passed] + new_files_list
        assert sorted(effective) == ["F", "G"], (
            f"new snapshot must list F and G exactly once each; got {effective}"
        )
        mock_compact_tomb.assert_not_called()


# ===========================================================================
# 10. Stats payload shape
# ===========================================================================


class TestResultShape:

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB)
    @patch(_P_READ_PARQUET)
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_returns_stats_dict_with_expected_keys(
        self, mock_check_write, MockSimple, mock_settings, mock_read_pq,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        dw = _build_writer()
        snap = _snapshot(
            [_resource("a"), _resource("b"), _resource("c")],
            tombstone="/d/tombstone.parquet", tombstone_rows=1,
        )
        mock_simple = _mk_simple_mock(snap)
        mock_simple.update.return_value = (
            {"resources": [{"file": "compacted"}], "snapshot_version": 2},
            "/snap/v2.json",
        )
        MockSimple.return_value = mock_simple
        mock_read_pq.return_value = _dv_frame(1)  # 1 tombstoned row

        new_res = [{"file": "c.parquet", "file_size": 5000, "columns": [{"name": "id"}]}]
        mock_compact_res.return_value = (
            3, 300, new_res, {"a", "b", "c"}, _dv_frame(0),
        )
        dw._get_table_config = MagicMock(return_value={})

        result = dw.compact("admin", "tbl", force_tombstones=True)

        expected_keys = {
            "query_id", "recorded_at", "organization", "super_name",
            "role_name", "table_name", "compression_level",
            "force_tombstones", "small_only",
            "files_before", "files_after", "files_compacted",
            "tombstone_rows_removed", "tombstone_files_consumed",
            "tombstone_files_rewritten", "tombstone_files_fully_dead",
            "new_resources", "sunset_files", "total_rows_written",
            "duration", "lineage", "counts",
        }
        missing = expected_keys - set(result.keys())
        assert not missing, f"result missing keys: {missing}"

        assert result["files_before"] == 3
        assert result["files_after"] == 1
        assert result["files_compacted"] == 3
        assert result["tombstone_rows_removed"] == 1
        assert result["tombstone_files_consumed"] == 1
        assert result["tombstone_files_rewritten"] == 0
        assert result["tombstone_files_fully_dead"] == 1
        assert result["sunset_files"] >= 3   # parquet sunsets + tombstone sunsets
        assert result["duration"] > 0
