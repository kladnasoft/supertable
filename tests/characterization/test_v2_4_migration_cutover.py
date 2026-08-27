"""Public writer/reader regression for the offline v2.4 cutover."""

from __future__ import annotations

import pyarrow as pa
import polars as pl

from supertable import redis_keys as RK
from supertable.data_reader import DataReader, engine
from supertable.data_writer import DataWriter
from supertable.redis_catalog import RedisCatalog
from supertable.row_identity import snapshot_proves_stable_rowids
from supertable.super_table import SuperTable
from supertable.tests.test_super_table_migration import (
    _seed_authentic_v2_4_active_table,
)


ORG = "org"
SUPER = "lake"
SIMPLE = "facts"
ROLE = "superadmin"


def test_v2_4_cutover_supports_first_public_write_and_read(
    hermetic_fakeredis,
):
    super_table = SuperTable(SUPER, ORG)
    catalog = RedisCatalog()
    # Preserve the fully bootstrapped root/RBAC estate; the historical-shape
    # helper owns only the legacy table leaf and immutable objects here.
    root_key = RK.meta_root(ORG, SUPER)
    bootstrapped_root = hermetic_fakeredis.get(root_key)
    assert bootstrapped_root is not None
    _seed_authentic_v2_4_active_table(
        super_table.storage,
        hermetic_fakeredis,
        catalog,
    )
    hermetic_fakeredis.set(root_key, bootstrapped_root)

    migrated = super_table.migrate_legacy_metadata(
        confirm_system_offline=True,
        expected_tables={SIMPLE},
    )
    assert migrated["migrated_tables"] == [SIMPLE]
    migrated_leaf = catalog.get_leaf(ORG, SUPER, SIMPLE)
    assert migrated_leaf is not None
    assert migrated_leaf["payload"]["_legacy_metadata_migration_version"] == 2
    assert snapshot_proves_stable_rowids(migrated_leaf["payload"]) is True

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
    assert successor["snapshot_version"] == 6
    assert successor["rowid_high_watermark"] == 12
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
    assert final_snapshot["snapshot_version"] == 8
    assert final_snapshot["rowid_high_watermark"] == 13
    assert snapshot_proves_stable_rowids(final_snapshot) is True
    assert final_snapshot["tombstone_format"] == 3
    # The overwrite and delete cross the compaction threshold. All three
    # deleted physical rows must be absorbed without resurrecting either the
    # migrated v2.4 deletion or the two post-cutover deletions.
    assert final_snapshot["tombstone"] is None
    assert final_snapshot["tombstone_rows"] == 0
    assert len(final_snapshot["resources"]) == 1
    assert final_snapshot["stats_rows"] > 0
    assert super_table.storage.exists(final_snapshot["stats_file"])
