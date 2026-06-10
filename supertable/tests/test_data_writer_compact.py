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
from unittest.mock import MagicMock, patch

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
_P_TOMB_THRESH   = f"{_MOD}._tombstone_threshold"
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


def _snapshot(resources: list, *, tombstones: dict | None = None) -> dict:
    snap = {
        "simple_name": "orders",
        "snapshot_version": 1,
        "resources": resources,
    }
    if tombstones is not None:
        snap["tombstones"] = tombstones
    return snap


def _mk_simple_mock(snap: dict, *, snap_path: str = "/snap/v1.json",
                    data_dir: str = "/d"):
    """Build a SimpleTable mock returning the given snapshot."""
    mock = MagicMock()
    mock.data_dir = data_dir
    mock.get_simple_table_snapshot.return_value = (snap, snap_path)
    # ``update`` returns (new_snapshot_dict, new_snapshot_path).
    # Callers patch this further if they care.
    mock.update.return_value = (
        {"resources": [{"file": "compacted.parquet"}], "snapshot_version": 2},
        "/snap/v2.json",
    )
    return mock


def _stub_settings():
    """Build a fake ``settings`` object for patching."""
    return MagicMock()


def _build_writer():
    """Construct a DataWriter via __new__ — skips the real __init__
    (which would touch SuperTable + RedisCatalog)."""
    from supertable.data_writer import DataWriter
    dw = DataWriter.__new__(DataWriter)
    dw.super_table = MagicMock(super_name="warehouse", organization="acme")
    dw.super_table.storage = MagicMock()
    dw.catalog = MagicMock()
    dw.catalog.acquire_simple_lock.return_value = "tok"
    dw.catalog.release_simple_lock.return_value = True
    dw.catalog.set_leaf_payload_cas.return_value = 1
    dw.catalog.bump_root.return_value = 1
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

        mock_check_write.assert_called_once_with(
            super_name="warehouse", organization="acme",
            role_name="admin", table_name="tbl",
        )

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

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES, return_value=(0, 0, [], set()))
    @patch(_P_COMPACT_TOMB)
    @patch(_P_TOMB_THRESH, return_value=1000)
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_force_tombstones_true_runs_unconditionally(
        self, mock_check_write, MockSimple, mock_settings, mock_thresh,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """force_tombstones=True ignores the natural threshold."""
        dw = _build_writer()
        # Snapshot has 1 tombstone — far below the 1000-row threshold.
        snap = _snapshot(
            [_resource("a"), _resource("b")],
            tombstones={
                "primary_keys": ["id"],
                "deleted_keys": [[42]],
                "total_tombstones": 1,
            },
        )
        MockSimple.return_value = _mk_simple_mock(snap)
        mock_compact_tomb.return_value = (1, [{"file": "rebuilt.parquet", "file_size": 100, "columns": []}], {"a"})
        dw._get_table_config = MagicMock(return_value={"primary_keys": ["id"]})

        dw.compact("admin", "tbl", force_tombstones=True)

        mock_compact_tomb.assert_called_once()

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES, return_value=(0, 0, [], set()))
    @patch(_P_COMPACT_TOMB)
    @patch(_P_TOMB_THRESH, return_value=1000)
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_force_tombstones_false_below_threshold_skips(
        self, mock_check_write, MockSimple, mock_settings, mock_thresh,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """force_tombstones=False + tombstones below threshold = skip."""
        dw = _build_writer()
        snap = _snapshot(
            [_resource("a")],
            tombstones={
                "primary_keys": ["id"],
                "deleted_keys": [[1], [2]],   # 2 tombstones, threshold=1000
                "total_tombstones": 2,
            },
        )
        MockSimple.return_value = _mk_simple_mock(snap)
        dw._get_table_config = MagicMock(return_value={"primary_keys": ["id"]})

        dw.compact("admin", "tbl", force_tombstones=False)

        mock_compact_tomb.assert_not_called()

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES, return_value=(0, 0, [], set()))
    @patch(_P_COMPACT_TOMB)
    @patch(_P_TOMB_THRESH, return_value=2)
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_force_tombstones_false_above_threshold_runs(
        self, mock_check_write, MockSimple, mock_settings, mock_thresh,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """force_tombstones=False + tombstones ≥ threshold = run."""
        dw = _build_writer()
        snap = _snapshot(
            [_resource("a")],
            tombstones={
                "primary_keys": ["id"],
                "deleted_keys": [[1], [2], [3]],  # 3 tombstones, threshold=2
                "total_tombstones": 3,
            },
        )
        MockSimple.return_value = _mk_simple_mock(snap)
        mock_compact_tomb.return_value = (3, [{"file": "rebuilt", "file_size": 1, "columns": []}], {"a"})
        dw._get_table_config = MagicMock(return_value={"primary_keys": ["id"]})

        dw.compact("admin", "tbl", force_tombstones=False)

        mock_compact_tomb.assert_called_once()

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES, return_value=(0, 0, [], set()))
    @patch(_P_COMPACT_TOMB)
    @patch(_P_TOMB_THRESH, return_value=1)
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_no_primary_keys_skips_tombstone_step(
        self, mock_check_write, MockSimple, mock_settings, mock_thresh,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """Without primary keys the tombstone step is a no-op."""
        dw = _build_writer()
        snap = _snapshot(
            [_resource("a")],
            tombstones={
                "primary_keys": [],
                "deleted_keys": [[1]],
                "total_tombstones": 1,
            },
        )
        MockSimple.return_value = _mk_simple_mock(snap)
        dw._get_table_config = MagicMock(return_value={})

        dw.compact("admin", "tbl", force_tombstones=True)

        mock_compact_tomb.assert_not_called()


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

        # No snapshot update, no leaf CAS, no root bump
        mock_simple.update.assert_not_called()
        dw.catalog.set_leaf_payload_cas.assert_not_called()
        dw.catalog.bump_root.assert_not_called()
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
    def test_leaf_cas_and_bump_root_after_successful_compaction(
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

        dw.catalog.set_leaf_payload_cas.assert_called_once()
        dw.catalog.bump_root.assert_called_once()


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
# compact() hands a model_df to simple_table.update(); update() derives
# the new snapshot's ``schema`` field from that frame's Polars dtypes.
# If the frame is empty / wrong-typed, the snapshot metadata gets
# corrupted (even though the Parquet files on disk are correct).
# These tests pin the resolution chain in ``_build_compact_model_df``.


class TestSchemaPreservation:

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_model_df_built_from_new_parquet_when_storage_succeeds(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """Happy path: model_df mirrors the schema of the first new
        compacted parquet file. simple_table.update receives a frame
        whose dtypes are the real post-compaction dtypes."""
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

        # update() received a non-empty schema frame with the real dtypes
        args, kwargs = mock_simple.update.call_args
        model_df = args[2]   # (new_resources, sunset, model_df, ...)
        assert model_df.width == 4, (
            f"model_df should have 4 columns, got {model_df.width} — "
            "schema would be corrupted"
        )
        assert set(model_df.columns) == {"id", "name", "amount", "ok"}
        # Dtypes match the source parquet, not Utf8
        dtype_map = dict(zip(model_df.columns, model_df.dtypes))
        assert dtype_map["id"] == pl.Int64
        assert dtype_map["amount"] == pl.Float64
        assert dtype_map["ok"] == pl.Boolean

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_fallback_to_prior_snapshot_schema_when_storage_read_fails(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """Storage read fails → reconstruct from last_simple_table["schema"]
        (Spark-style entries). Compaction preserves schema by definition
        so the pre-compaction schema is correct."""
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
        model_df = args[2]
        assert model_df.width == 3, (
            "fallback should have reconstructed all 3 columns from snapshot schema"
        )
        assert set(model_df.columns) == {"id", "amount", "flag"}
        dtype_map = dict(zip(model_df.columns, model_df.dtypes))
        assert dtype_map["id"] == pl.Int64       # "long" → Int64
        assert dtype_map["amount"] == pl.Float64  # "double" → Float64
        assert dtype_map["flag"] == pl.Boolean    # "boolean" → Boolean

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_empty_frame_when_no_schema_anywhere(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """Pathological case: storage read fails AND snapshot has no
        schema field. Falls back to an empty frame — the snapshot's
        existing schema (None/missing) is left as-is."""
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
        model_df = args[2]
        # Empty frame is acceptable here — it would leave the schema
        # blank in the new snapshot, which is no worse than the
        # pre-fix behaviour (silent corruption).
        assert isinstance(model_df, pl.DataFrame)

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB, return_value=(0, [], set()))
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_multi_chunk_schemas_union_correctly(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """Regression: compact_resources may produce multiple chunks
        with different schemas (when files have schema evolution).
        ``_build_compact_model_df`` must read ALL new files and union
        their schemas — not just the first — otherwise the snapshot's
        schema field is missing columns that appear in later chunks."""
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
        model_df = args[2]
        # CRITICAL: model_df must have the UNION of both chunks' schemas
        assert model_df.width == 3, (
            f"model_df must union schemas from all chunks; got width={model_df.width}"
        )
        assert set(model_df.columns) == {"id", "name", "email"}, (
            f"missing columns: expected {{id, name, email}}, got {set(model_df.columns)}"
        )

    def test_spark_to_polars_type_map_covers_common_types(self):
        """The conversion map must cover the dtypes that
        ``_schema_list_from_polars_df`` in ``simple_table`` emits, so
        a round-trip (Polars → snapshot → fallback model_df) is lossless."""
        from supertable.data_writer import DataWriter
        m = DataWriter._SPARK_TYPE_TO_POLARS
        # Every common dtype must map to a Polars attribute that exists.
        for spark, pl_name in m.items():
            assert hasattr(pl, pl_name), (
                f"{spark!r} → {pl_name!r} but polars.{pl_name} does not exist"
            )


# ===========================================================================
# 9. Two-phase aggregation — tombstone output must not be re-sunset
# ===========================================================================


class TestTwoPhaseAggregation:
    """Regression for the duplicate-file bug.

    A file produced by Phase A (compact_tombstones) can be picked up
    as a small-file candidate by Phase B (compact_resources) and
    sunset there. If we leave it in ``all_new_resources``, the final
    snapshot lists it AND the file is on the GC queue → 30 min later
    GC deletes a file the snapshot still references.
    """

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB)
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_tombstone_output_resunset_by_phase_b_not_in_new_resources(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        dw = _build_writer()

        # Storage.read_parquet returns an arrow table for schema derivation
        import pyarrow as pa
        dw.super_table.storage.read_parquet.return_value = pa.table(
            {"id": pa.array([1], type=pa.int64())}
        )

        # Snapshot has A (with tombstones), B, C
        snap = _snapshot(
            [_resource("A", file_size=2_000_000),
             _resource("B", file_size=1_000_000),
             _resource("C", file_size=1_000_000)],
            tombstones={
                "primary_keys": ["id"],
                "deleted_keys": [[42]],
                "total_tombstones": 1,
            },
        )
        mock_simple = _mk_simple_mock(snap)
        MockSimple.return_value = mock_simple

        # Phase A: tombstone compaction rewrites A → F
        F_resource = {
            "file": "F", "file_size": 1_500_000,
            "rows": 50, "columns": 1, "stats": None,
        }
        mock_compact_tomb.return_value = (1, [F_resource], {"A"})

        # Phase B: compact_resources sees B, C, F (all small) and merges
        # them into G. F is in the sunset set — that's the bug-trigger.
        G_resource = {
            "file": "G", "file_size": 3_500_000,
            "rows": 150, "columns": 1, "stats": None,
        }
        mock_compact_res.return_value = (3, 150, [G_resource], {"B", "C", "F"})

        dw._get_table_config = MagicMock(return_value={"primary_keys": ["id"]})

        dw.compact("admin", "tbl", force_tombstones=True)

        # Inspect the new_resources passed to simple_table.update.
        args, kwargs = mock_simple.update.call_args
        new_resources_passed = args[0]
        sunset_passed = args[1]

        new_files = {r["file"] for r in new_resources_passed}
        # CRITICAL: F must NOT appear in new_resources because it's
        # also sunset. Only G should be there.
        assert new_files == {"G"}, (
            f"new_resources must exclude files in sunset; got new={new_files}, "
            f"sunset={sunset_passed}"
        )
        # Sunset is the union of both phases
        assert sunset_passed == {"A", "B", "C", "F"}

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB)
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_tombstone_output_survives_when_phase_b_does_not_sunset_it(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        """The benign case: tombstone-output F is large (or otherwise
        not picked up by Phase B). F must remain in new_resources."""
        dw = _build_writer()

        import pyarrow as pa
        dw.super_table.storage.read_parquet.return_value = pa.table(
            {"id": pa.array([1], type=pa.int64())}
        )

        snap = _snapshot(
            [_resource("A", file_size=2_000_000),
             _resource("B", file_size=500_000)],
            tombstones={
                "primary_keys": ["id"],
                "deleted_keys": [[42]],
                "total_tombstones": 1,
            },
        )
        mock_simple = _mk_simple_mock(snap)
        MockSimple.return_value = mock_simple

        F_resource = {
            "file": "F", "file_size": 20_000_000,  # large
            "rows": 50, "columns": 1, "stats": None,
        }
        mock_compact_tomb.return_value = (1, [F_resource], {"A"})

        # Phase B only sees B (small) and rewrites it → G. F is too
        # large for the small-file gate.
        G_resource = {
            "file": "G", "file_size": 500_000,
            "rows": 50, "columns": 1, "stats": None,
        }
        mock_compact_res.return_value = (1, 50, [G_resource], {"B"})

        dw._get_table_config = MagicMock(return_value={"primary_keys": ["id"]})

        dw.compact("admin", "tbl", force_tombstones=True)

        args, kwargs = mock_simple.update.call_args
        new_resources_passed = args[0]
        new_files = {r["file"] for r in new_resources_passed}
        # Both F and G survive — neither was sunset
        assert new_files == {"F", "G"}, f"unexpected new={new_files}"


# ===========================================================================
# 10. Stats payload shape
# ===========================================================================


class TestResultShape:

    @patch(_P_AUDIT)
    @patch(_P_MON_WRITER)
    @patch(_P_MIRROR)
    @patch(_P_COMPACT_RES)
    @patch(_P_COMPACT_TOMB)
    @patch(_P_SETTINGS, new_callable=_stub_settings)
    @patch(_P_SIMPLE_TABLE)
    @patch(_P_CHECK_WRITE)
    def test_returns_stats_dict_with_expected_keys(
        self, mock_check_write, MockSimple, mock_settings,
        mock_compact_tomb, mock_compact_res, MockMirror, MockMW, mock_audit,
    ):
        dw = _build_writer()
        snap = _snapshot(
            [_resource("a"), _resource("b"), _resource("c")],
            tombstones={"primary_keys": ["id"], "deleted_keys": [[1]], "total_tombstones": 1},
        )
        mock_simple = _mk_simple_mock(snap)
        mock_simple.update.return_value = (
            {"resources": [{"file": "compacted"}], "snapshot_version": 2},
            "/snap/v2.json",
        )
        MockSimple.return_value = mock_simple

        new_res = [{"file": "c.parquet", "file_size": 5000, "columns": [{"name": "id"}]}]
        mock_compact_res.return_value = (3, 300, new_res, {"a", "b", "c"})
        mock_compact_tomb.return_value = (1, [], {"a"})  # 1 tombstoned row, no new files
        dw._get_table_config = MagicMock(return_value={"primary_keys": ["id"]})

        result = dw.compact("admin", "tbl", force_tombstones=True)

        expected_keys = {
            "query_id", "recorded_at", "organization", "super_name",
            "role_name", "table_name", "compression_level",
            "force_tombstones", "small_only",
            "files_before", "files_after", "files_compacted",
            "tombstone_rows_removed", "tombstone_files_rewritten",
            "new_resources", "sunset_files", "total_rows_written",
            "duration", "lineage",
        }
        missing = expected_keys - set(result.keys())
        assert not missing, f"result missing keys: {missing}"

        assert result["files_before"] == 3
        assert result["files_after"] == 1
        assert result["files_compacted"] == 3
        assert result["tombstone_rows_removed"] == 1
        assert result["sunset_files"] >= 3   # parquet sunsets + tombstone sunsets
        assert result["duration"] > 0
