"""Public reads preserve deletion semantics through subsequent writes."""

from __future__ import annotations

from dataclasses import replace

import pyarrow as pa
import polars as pl

import supertable.data_writer as writer_module
from supertable.data_reader import DataReader, engine
from supertable.data_writer import DataWriter
from supertable.redis_catalog import RedisCatalog
from supertable.row_identity import snapshot_proves_stable_rowids
from supertable.super_table import SuperTable


ORG = "org"
SUPER = "lake"
SIMPLE = "facts"
ROLE = "superadmin"


def test_append_overwrite_and_delete_preserve_existing_deletions(monkeypatch):
    monkeypatch.setattr(
        writer_module,
        "settings",
        replace(writer_module.settings, SUPERTABLE_DV_V3_WRITES_ENABLED=True),
    )
    super_table = SuperTable(SUPER, ORG)
    catalog = RedisCatalog()
    writer = DataWriter(SUPER, ORG)
    writer.write(
        role_name=ROLE,
        simple_name=SIMPLE,
        data=pa.table({"id": [1, 2], "payload": [b"a", b"x" * 257]}),
        overwrite_columns=[],
    )
    writer.configure_table(
        role_name=ROLE,
        simple_name=SIMPLE,
        max_overlapping_files=3,
        deletion_vector_format=3,
        confirm_dv_v3_reader_fleet=True,
    )
    writer.write(
        role_name=ROLE,
        simple_name=SIMPLE,
        data=pa.table({"id": [1]}),
        overwrite_columns=["id"],
        delete_only=True,
    )
    initial_leaf = catalog.get_leaf(ORG, SUPER, SIMPLE)
    assert initial_leaf is not None
    initial_snapshot = initial_leaf["payload"]
    assert snapshot_proves_stable_rowids(initial_snapshot) is True
    assert initial_snapshot["tombstone_rows"] == 1

    write_result = DataWriter(SUPER, ORG).write(
        role_name=ROLE,
        simple_name=SIMPLE,
        data=pa.table({"id": [3], "payload": [b"new"]}),
        overwrite_columns=[],
    )
    assert write_result is not None and write_result[2] == 1

    frame, status, message = DataReader(
        super_name=SUPER,
        organization=ORG,
        query=f"SELECT id, payload FROM {SIMPLE} ORDER BY id",
    ).execute(role_name=ROLE, with_scan=False, engine=engine.AUTO)
    assert str(status).endswith("OK"), message
    result = frame if isinstance(frame, pl.DataFrame) else pl.from_pandas(frame)
    assert result.to_dicts() == [
        {"id": 2, "payload": b"x" * 257},
        {"id": 3, "payload": b"new"},
    ]

    leaf = catalog.get_leaf(ORG, SUPER, SIMPLE)
    assert leaf is not None
    successor = leaf["payload"]
    assert successor["snapshot_version"] == initial_snapshot["snapshot_version"] + 1
    assert successor["rowid_high_watermark"] == initial_snapshot["rowid_high_watermark"] + 1
    assert snapshot_proves_stable_rowids(successor) is True
    assert successor["tombstone_format"] == 3
    assert successor["stats_rows"] > 0
    assert super_table.storage.exists(successor["stats_file"])
    assert len(successor["resources"]) == 2

    overwrite_result = DataWriter(SUPER, ORG).write(
        role_name=ROLE,
        simple_name=SIMPLE,
        data=pa.table({"id": [2], "payload": [b"updated"]}),
        overwrite_columns=["id"],
    )
    assert overwrite_result is not None and overwrite_result[2] == 1
    delete_result = DataWriter(SUPER, ORG).write(
        role_name=ROLE,
        simple_name=SIMPLE,
        data=pa.table({"id": [3]}),
        overwrite_columns=["id"],
        delete_only=True,
    )
    assert delete_result is not None

    frame, status, message = DataReader(
        super_name=SUPER,
        organization=ORG,
        query=f"SELECT id, payload FROM {SIMPLE} ORDER BY id",
    ).execute(role_name=ROLE, with_scan=False, engine=engine.AUTO)
    assert str(status).endswith("OK"), message
    result = frame if isinstance(frame, pl.DataFrame) else pl.from_pandas(frame)
    assert result.to_dicts() == [{"id": 2, "payload": b"updated"}]

    final_leaf = catalog.get_leaf(ORG, SUPER, SIMPLE)
    assert final_leaf is not None
    final_snapshot = final_leaf["payload"]
    assert final_snapshot["snapshot_version"] == initial_snapshot["snapshot_version"] + 3
    assert final_snapshot["rowid_high_watermark"] == initial_snapshot["rowid_high_watermark"] + 2
    assert snapshot_proves_stable_rowids(final_snapshot) is True
    assert final_snapshot["tombstone_format"] == 3
    # The overwrite and delete cross the compaction threshold. All three
    # deleted physical rows must be absorbed without resurrecting either the
    # initial deletion or the two subsequent deletions.
    assert final_snapshot["tombstone"] is None
    assert final_snapshot["tombstone_rows"] == 0
    assert len(final_snapshot["resources"]) == 1
    assert final_snapshot["stats_rows"] > 0
    assert super_table.storage.exists(final_snapshot["stats_file"])
