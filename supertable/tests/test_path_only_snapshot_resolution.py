"""STREAD-001: a path-only leaf must still yield its access controls.

A Redis leaf holds a snapshot *path* and, as an optimisation, an inline copy of
the snapshot under ``payload``. The inline copy is best-effort — every publish
site (``DataWriter`` twice, ``SimpleTable.update``) falls back to
``set_leaf_path_cas``, path only, when the payload CAS raises, and
``RedisCatalog.set_leaf_payload_cas`` falls back the same way if its Lua script
is not registered. A leaf without a payload is a state the writer is built to
produce.

The reader used to take the deletion vector and the share row filter from
``payload`` only. On a path-only leaf the ``isinstance`` check simply failed and
the read continued with **neither**: deleted and superseded rows came back, and
a linked share served rows outside its filter. Nothing raised, so nothing was
logged and no status reflected it — the two existing fail-closed handlers could
not fire because there was no exception.

Meanwhile the estimator resolved resources from the path regardless, so the two
halves of the same read disagreed about which snapshot they were reading.

These tests exercise the resolver directly rather than through Redis: the
distinction under test is which *leaf shape* is handled, and that is decided
entirely by this method.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from supertable.data_reader import DataReader

SNAPSHOT = {
    "resources": [{"file": "data/part-0.parquet", "file_size": 1024}],
    "tombstone": ["tomb/dv-0.parquet", "tomb/dv-1.parquet"],
    "_row_filter": "value >= 100",
    "schema": {"kid": "Int64", "value": "Int64"},
}


@pytest.fixture
def reader():
    """A DataReader with only the fields the resolver touches.

    Built without __init__ on purpose: the resolver needs an organization and a
    catalog, not a Redis connection or a storage backend.
    """
    dr = DataReader.__new__(DataReader)
    dr.organization = "org"
    return dr


def _resolve(reader, leaf, on_disk=None, side_effect=None):
    catalog = MagicMock()
    catalog.get_leaf.return_value = leaf
    with patch("supertable.super_table.SuperTable") as super_table:
        reader_fn = super_table.return_value.read_simple_table_snapshot
        if side_effect is not None:
            reader_fn.side_effect = side_effect
        else:
            reader_fn.return_value = on_disk
        return reader._resolve_snapshot(catalog, "warehouse", "cachecheck")


class TestPathOnlyLeafStillCarriesItsControls:

    def test_inline_payload_is_used_directly(self, reader):
        """The fast path is unchanged: a usable inline payload skips storage."""
        resolved = _resolve(reader, {"payload": SNAPSHOT, "path": "snap.json"})
        assert resolved["tombstone"] == SNAPSHOT["tombstone"]
        assert resolved["_row_filter"] == "value >= 100"

    def test_a_path_only_leaf_resolves_from_storage(self, reader):
        """The regression: no payload at all.

        This is the shape the audit injected and the shape every publish-site
        fallback produces. Both controls have to come back.
        """
        resolved = _resolve(reader, {"path": "snap.json"}, on_disk=SNAPSHOT)
        assert resolved["tombstone"] == SNAPSHOT["tombstone"]
        assert resolved["_row_filter"] == "value >= 100"

    def test_every_deletion_vector_part_survives(self, reader):
        """Dropping one part resurrects exactly the rows it recorded."""
        resolved = _resolve(reader, {"path": "snap.json"}, on_disk=SNAPSHOT)
        assert len(resolved["tombstone"]) == 2

    def test_an_unusable_payload_falls_through_to_the_path(self, reader):
        """Agreement with the estimator is the point.

        The estimator treats an inline payload as usable only when it carries a
        ``resources`` list, and reads the snapshot from the path otherwise. The
        reader now applies the same predicate, so a payload that the estimator
        would bypass cannot leave the reader looking at a different snapshot —
        which is how the deletion vector went missing while the file list did
        not.
        """
        resolved = _resolve(
            reader,
            {"payload": {"stats_file": "s.parquet"}, "path": "snap.json"},
            on_disk=SNAPSHOT,
        )
        assert resolved["tombstone"] == SNAPSHOT["tombstone"]


class TestUnreadableMetadataFailsClosed:

    @pytest.mark.parametrize("error", [
        FileNotFoundError("snapshot not found"),
        ValueError("snapshot is empty"),
        OSError("storage unreachable"),
    ])
    def test_it_raises_rather_than_returning_an_empty_snapshot(self, reader, error):
        """"Could not determine the deletion vector" is not "there is no vector".

        Returning ``{}`` here would look exactly like a table with no deletions
        and would serve every tombstoned row. The caller wraps whatever this
        raises into DeletionVectorUnavailable, so raising is the contract.
        """
        with pytest.raises(type(error)):
            _resolve(reader, {"path": "snap.json"}, side_effect=error)

    def test_the_caller_turns_that_into_deletion_vector_unavailable(self):
        """The resolver sits inside the handler that already exists for this."""
        import inspect

        from supertable.data_reader import DataReader as DR

        source = inspect.getsource(DR.execute)
        resolver_at = source.index("_resolve_snapshot")
        raise_at = source.index("DeletionVectorUnavailable")
        assert resolver_at < raise_at, (
            "the resolver must be inside the try block that raises "
            "DeletionVectorUnavailable, or an unreadable snapshot fails open"
        )


class TestShapesThatAreNotFailures:

    def test_a_table_with_neither_payload_nor_path_is_empty_not_an_error(self, reader):
        """A table that has never been written has no deletions to miss."""
        assert _resolve(reader, {}) == {}

    def test_a_non_dict_leaf_is_empty_not_an_error(self, reader):
        assert _resolve(reader, None) == {}

    def test_a_payload_only_leaf_with_no_path_is_used_as_is(self, reader):
        """No path to fall back to; the payload is all there is."""
        resolved = _resolve(reader, {"payload": {"tombstone": ["t.parquet"]}})
        assert resolved["tombstone"] == ["t.parquet"]
