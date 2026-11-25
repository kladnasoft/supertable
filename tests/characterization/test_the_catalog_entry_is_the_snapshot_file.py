"""
After a write, the table's catalog entry holds what its snapshot file holds.

Every commit stores the snapshot twice: as a file, and in the table's catalog
entry so that readers need no storage read.  The script that stored the second
copy re-encoded it, and on a real Redis the copy was not the snapshot: empty
lists came back as ``{}`` (the ``overwrite_columns`` of every append's lineage,
the ``resources`` of an empty table), large integers rounded.  It is stored as
text now, and the snapshot file is written on one line instead of indented.

The real writer and reader run here over the hermetic fake Redis on LOCAL
storage.  After every kind of write the entry must be the file's document,
value for value, and the table must read back the rows it was given.

``supertable/tests/test_the_catalog_entry_holds_the_snapshot_it_was_given.py``
is the unit side; ``test_suite/test_catalog_commit.py`` repeats it on a real
Redis, where the old script's damage can be shown.
"""

from __future__ import annotations

import json
import uuid
from types import SimpleNamespace

import polars as pl
import pytest

from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine.engine_enum import Engine
from supertable.redis_catalog import RedisCatalog
from supertable.simple_table import SimpleTable
from supertable.storage.storage_interface import json_text
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
TABLE = "orders"


def _same(a, b) -> bool:
    """Equal, types and key order included."""
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return list(a) == list(b) and all(_same(a[k], b[k]) for k in a)
    if isinstance(a, list):
        return len(a) == len(b) and all(_same(x, y) for x, y in zip(a, b))
    if isinstance(a, float):
        return repr(a) == repr(b)
    return a == b


def _rows(first: int, count: int = 3) -> pl.DataFrame:
    ids = list(range(first, first + count))
    return pl.DataFrame({"id": ids, "region": ["eu" if i % 2 else "us" for i in ids],
                         "amount": [i * 10 for i in ids]})


@pytest.fixture
def lake(hermetic_fakeredis):
    engine_duckdb.reset_shared_duckdb_state()
    sup = f"entry_{uuid.uuid4().hex[:12]}"
    super_table = SuperTable(sup, ORG)
    writer = DataWriter(super_name=sup, organization=ORG)
    catalog = RedisCatalog()

    def leaf() -> dict:
        return catalog.get_leaf(ORG, sup, TABLE)

    def on_storage() -> dict:
        return super_table.storage.read_json(leaf()["path"])

    def write(frame: pl.DataFrame, overwrite_columns=(), **options):
        return writer.write(role_name=ADMIN, simple_name=TABLE,
                            overwrite_columns=list(overwrite_columns), data=frame, **options)

    def read(sql: str = f"SELECT id, region, amount FROM {TABLE} ORDER BY id"):
        frame, status, message = DataReader(
            super_name=sup, organization=ORG, query=sql,
        ).execute(role_name=ADMIN, engine=Engine.DUCKDB)
        assert status is Status.OK, message
        return frame.to_dicts()

    yield SimpleNamespace(sup=sup, super_table=super_table, writer=writer, catalog=catalog,
                          leaf=leaf, on_storage=on_storage, write=write, read=read)
    engine_duckdb.reset_shared_duckdb_state()


def _expected(ids, amounts=None) -> list:
    amounts = amounts or {}
    return [{"id": i, "region": "eu" if i % 2 else "us", "amount": amounts.get(i, i * 10)}
            for i in sorted(ids)]


# ---------------------------------------------------------------------------
# Every kind of write
# ---------------------------------------------------------------------------

def test_the_entry_is_the_file_s_document_after_every_kind_of_write(lake):
    seen = []

    def check(label: str) -> None:
        leaf, document = lake.leaf(), lake.on_storage()
        assert _same(leaf["payload"], document), label
        assert isinstance(leaf["payload"]["resources"], list), label
        seen.append((label, leaf["version"]))

    lake.write(_rows(0))
    check("the first write")
    lake.write(_rows(3))
    check("an append")
    lake.write(pl.DataFrame({"id": [1, 4], "region": ["eu", "us"], "amount": [-1, -4]}),
               overwrite_columns=["id"])
    check("an upsert")
    lake.write(pl.DataFrame({"id": [0]}), overwrite_columns=["id"], delete_only=True)
    check("a delete")
    lake.write(_rows(10), lineage={"source_type": "api_upload", "source_files": [], "tags": {},
                                   "job_id": 1234567890123456789, "ratio": 0.1,
                                   "note": "árvíztűrő 日本語"})
    check("a write with lineage")
    lake.writer.compact(role_name=ADMIN, simple_name=TABLE)
    check("a compaction")

    assert [version for _label, version in seen] == list(range(seen[0][1], seen[0][1] + 6))
    assert lake.read() == _expected([1, 2, 3, 4, 5, 10, 11, 12], {1: -1, 4: -4})


def test_the_lineage_of_an_append_keeps_its_empty_list(lake):
    lake.write(_rows(0))

    lineage = lake.leaf()["payload"]["lineage"]

    assert _same(lineage["overwrite_columns"], [])
    assert _same(lineage, lake.on_storage()["lineage"])


def test_a_table_emptied_by_a_delete_is_read_from_its_entry(lake):
    """No file is left, ``resources`` is an empty list, and an entry whose
    resources are a list is what every reader uses without a storage read."""
    lake.write(_rows(0))
    lake.write(pl.DataFrame({"id": [0]}), overwrite_columns=[], delete_only=True)

    leaf = lake.leaf()
    assert _same(leaf["payload"]["resources"], [])
    assert _same(leaf["payload"], lake.on_storage())
    assert lake.read() == []
    snapshot, _path = SimpleTable(lake.super_table, TABLE, create_if_missing=False
                                  ).get_simple_table_snapshot()
    assert snapshot == leaf["payload"]

    lake.write(_rows(20))
    assert lake.read() == _expected([20, 21, 22])


def test_the_snapshot_file_is_one_line_of_the_same_document(lake):
    lake.write(_rows(0))
    lake.write(_rows(3))

    leaf = lake.leaf()
    with open(leaf["path"], "rb") as handle:
        stored = handle.read()

    assert b"\n" not in stored
    assert stored == json_text(leaf["payload"]).encode("utf-8")
    assert _same(json.loads(stored), leaf["payload"])


# ---------------------------------------------------------------------------
# A snapshot the entry cannot hold
# ---------------------------------------------------------------------------

def test_a_write_whose_lineage_is_not_a_finite_number_still_succeeds(lake):
    """As on a real Redis before: the entry is stored without the snapshot,
    readers follow its path to the file, and the next write stores one again."""
    lake.write(_rows(0))

    result = lake.write(_rows(3), lineage={"source_type": "manual", "ratio": float("nan")})

    assert result[2] == 3, "three rows inserted"
    leaf = lake.leaf()
    assert "payload" not in leaf
    assert leaf["path"].endswith(".json")
    assert lake.read() == _expected(range(6))
    snapshot, _path = SimpleTable(lake.super_table, TABLE, create_if_missing=False
                                  ).get_simple_table_snapshot()
    assert len(snapshot["resources"]) == 2

    lake.write(_rows(6))
    leaf = lake.leaf()
    assert len(leaf["payload"]["resources"]) == 3
    assert _same(leaf["payload"], lake.on_storage())
    assert lake.read() == _expected(range(9))
