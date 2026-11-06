"""A table's column count in its stats and its meta, on the real write path.

``MetaReader.get_table_stats`` and ``get_super_meta`` report one ``cols_count``
per table.  This drives the real ``DataWriter``, the catalog's Lua (on
fakeredis) and LOCAL storage, so the count is taken from the snapshots and leaf
payloads a write actually publishes, and checked against the columns
``get_table_schema`` lists for the same table.

A version without files declares its table's columns in its ``schema``: an
empty write to a table without files publishes one, and so does
dataisland-core's rollback to version 0, which keeps the columns of the
version it replaces (built here with ``SimpleTable.update``: every file sunset,
the schema kept).  The reader serves those columns as a typed empty relation;
counting only resources reported 0 for them.
"""

from __future__ import annotations

import json

import polars as pl

from supertable.data_writer import DataWriter
from supertable.meta_reader import MetaReader
from supertable.simple_table import SimpleTable
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "meta"
ROLE = "superadmin"


def test_stats_and_meta_count_the_columns_the_schema_lists():
    st = SuperTable(SUPER, ORG)  # bootstrap super table + default superadmin role/user
    dw = DataWriter(super_name=SUPER, organization=ORG)
    dw.write(role_name=ROLE, simple_name="filled", overwrite_columns=[],
             data=pl.DataFrame({"id": [1, 2], "amount": [1.5, 2.5], "region": ["a", "b"]}))
    dw.write(role_name=ROLE, simple_name="empty_write", overwrite_columns=[],
             data=pl.DataFrame({"id": pl.Series([], dtype=pl.Int64),
                                "name": pl.Series([], dtype=pl.Utf8)}))
    dw.write(role_name=ROLE, simple_name="rolled_back", overwrite_columns=[],
             data=pl.DataFrame({"k": [1], "v": ["x"], "w": [0.5], "z": [True]}))
    rolled = SimpleTable(st, "rolled_back", create_if_missing=False)
    head, head_path = rolled.get_simple_table_snapshot()
    snapshot, path = rolled.update(
        [], {r["file"] for r in head["resources"]}, None,
        last_snapshot=json.loads(json.dumps(head)), last_snapshot_path=head_path,
    )
    rolled.catalog.set_leaf_payload_cas(ORG, SUPER, "rolled_back", snapshot, path)
    rolled.catalog.bump_root(ORG, SUPER)
    SimpleTable(st, "bootstrap")  # version 0: no files, no columns

    reader = MetaReader(SUPER, ORG)
    meta = {t["name"]: t for t in reader.get_super_meta(ROLE)["super"]["tables"]}
    declared = {}
    for name, files in (("filled", 1), ("empty_write", 0), ("rolled_back", 0), ("bootstrap", 0)):
        declared[name] = len(reader.get_table_schema(name, ROLE)[0])
        stats = reader.get_table_stats(name, ROLE)
        assert meta[name]["files"] == files, name
        assert meta[name]["cols_count"] == declared[name], name
        assert [item["cols_count"] for item in stats] == [declared[name]], name

    assert declared["empty_write"] > 0 and declared["rolled_back"] > 0
    assert declared["bootstrap"] == 0
