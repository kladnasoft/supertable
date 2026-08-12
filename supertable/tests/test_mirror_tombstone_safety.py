from unittest.mock import MagicMock, patch

import pytest

from supertable.mirroring.mirror_formats import MirrorFormats, MirrorSyncError
from supertable.mirroring.mirror_parquet import write_parquet_table
from supertable.mirroring.mirror_delta import write_delta_table
from supertable.mirroring.mirror_iceberg import write_iceberg_table


@pytest.mark.parametrize(
    "snapshot",
    [
        {"resources": [], "tombstone": "table/tombstone/deleted.parquet", "tombstone_rows": 1},
        {"resources": [], "tombstone": None, "tombstone_rows": 1},
    ],
)
def test_mirror_rejects_any_active_deletion_vector(snapshot):
    """A physical-resource mirror must never resurrect tombstoned rows."""
    super_table = MagicMock()

    with patch(
        "supertable.mirroring.mirror_formats.write_parquet_table"
    ) as write_parquet:
        with pytest.raises(RuntimeError, match="active deletion vector"):
            MirrorFormats.mirror_if_enabled(
                super_table,
                "events",
                snapshot,
                mirrors=["PARQUET"],
            )

    super_table.storage.makedirs.assert_not_called()
    write_parquet.assert_not_called()


def test_mirror_accepts_a_fully_drained_snapshot():
    super_table = MagicMock()
    snapshot = {"resources": [], "tombstone": None, "tombstone_rows": 0}

    with patch(
        "supertable.mirroring.mirror_formats.write_parquet_table"
    ) as write_parquet:
        MirrorFormats.mirror_if_enabled(
            super_table,
            "events",
            snapshot,
            mirrors=["PARQUET"],
        )

    write_parquet.assert_called_once_with(super_table, "events", snapshot)


def test_dispatch_reports_exact_failed_and_completed_formats():
    super_table = MagicMock()
    snapshot = {"resources": [], "tombstone": None, "tombstone_rows": 0}
    with (
        patch("supertable.mirroring.mirror_formats.write_delta_table") as delta,
        patch(
            "supertable.mirroring.mirror_formats.write_parquet_table",
            side_effect=OSError("PUT failed"),
        ) as parquet,
    ):
        with pytest.raises(MirrorSyncError) as raised:
            MirrorFormats.mirror_if_enabled(
                super_table,
                "events",
                snapshot,
                mirrors=["DELTA", "PARQUET"],
            )

    assert raised.value.failed_format == "PARQUET"
    assert raised.value.completed_formats == ("DELTA",)
    assert isinstance(raised.value.cause, OSError)
    assert "PUT failed" in str(raised.value)
    delta.assert_called_once()
    parquet.assert_called_once()


def test_parquet_listing_failure_cannot_report_success():
    storage = MagicMock(spec=["makedirs", "list_files"])
    storage.list_files.side_effect = OSError("backend unavailable")
    super_table = MagicMock(
        organization="org", super_name="lake", storage=storage,
    )

    with pytest.raises(OSError, match="backend unavailable"):
        write_parquet_table(
            super_table,
            "events",
            {"resources": []},
        )


def test_parquet_obsolete_delete_failure_cannot_report_success():
    class Storage:
        def makedirs(self, path):
            pass

        def list_files(self, path, pattern="*"):
            return [f"{path}/old.parquet"]

        def delete(self, path):
            raise OSError("delete denied")

    super_table = MagicMock(
        organization="org", super_name="lake", storage=Storage(),
    )
    with pytest.raises(OSError, match="delete denied"):
        write_parquet_table(
            super_table,
            "events",
            {"resources": []},
        )


def test_parquet_silent_delete_noop_cannot_report_success():
    class Storage:
        def makedirs(self, path):
            pass

        def list_files(self, path, pattern="*"):
            return [f"{path}/old.parquet"]

        def delete(self, path):
            pass

    super_table = MagicMock(
        organization="org", super_name="lake", storage=Storage(),
    )
    with pytest.raises(RuntimeError, match="remain visible"):
        write_parquet_table(
            super_table,
            "events",
            {"resources": []},
        )


def test_iceberg_standard_failure_is_not_hidden_by_legacy_fallback():
    super_table = MagicMock()
    with (
        patch(
            "supertable.mirroring.mirror_iceberg._write_iceberg_standard",
            side_effect=OSError("metadata PUT failed"),
        ),
        patch(
            "supertable.mirroring.mirror_iceberg._write_iceberg_table_iceberg_lite"
        ) as legacy,
    ):
        with pytest.raises(OSError, match="metadata PUT failed"):
            write_iceberg_table(super_table, "events", {"resources": []})

    legacy.assert_not_called()


def test_delta_publishes_remove_log_before_physical_cleanup():
    events = []

    class Storage:
        def makedirs(self, path):
            pass

        def list_files(self, path, pattern="*"):
            return [f"{path}/old.parquet"]

        def copy(self, source, destination):
            events.append(("copy", destination))

        def exists(self, path):
            return False

        def write_bytes(self, path, payload):
            events.append(("commit", path))

        def delete(self, path):
            events.append(("delete", path))

    super_table = MagicMock(
        organization="org", super_name="lake", storage=Storage(),
    )
    write_delta_table(
        super_table,
        "events",
        {
            "snapshot_version": 7,
            "schema": [{"name": "id", "type": "Int64"}],
            "resources": [
                {"file": "source/new.parquet", "file_size": 10, "rows": 1}
            ],
        },
    )

    commit_index = next(i for i, event in enumerate(events) if event[0] == "commit")
    delete_index = next(i for i, event in enumerate(events) if event[0] == "delete")
    assert commit_index < delete_index
