"""
A table that is deleted from and read keeps a handful of deletion-vector
tables on the reading connection, not one for every delete.

A read anti-joins the table's deletion vector, which the engine keeps as a
DuckDB table per version of the vector (``TombstoneCache``), at most
``SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_MAX_PER_TABLE`` versions for a table.  A
delete adds a part to the vector, so the next read sees a new version.

Which table a version belongs to was read from the text before the cache key's
last separator.  For a vector of more than one part the key is the text of the
whole list of parts, so that text was different for every version: every
version counted as a table of its own, the cap never applied, and every version
stayed until its idle time (300 s) ran out.  A reader thread of a table under
one delete every 165 ms held 1,750 vector tables and 541 MB for it.

Here the real writer deletes and the real reader reads, over the hermetic fake
Redis; what is counted is what is left on the thread's connection.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import polars as pl
import pytest

from supertable.config.settings import settings
from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine.engine_enum import Engine
from supertable.simple_table import SimpleTable
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
ROWS = 200


def _count(lake: str, table: str) -> int:
    frame, status, message = DataReader(
        super_name=lake, organization=ORG, query=f"SELECT COUNT(*) AS n FROM {table}",
    ).execute(role_name=ADMIN, engine=Engine.DUCKDB)
    assert status == Status.OK, message
    return int(frame["n"][0])


def _delete(lake, table: str, key: int) -> None:
    lake.writer.write(role_name=ADMIN, simple_name=table, overwrite_columns=["id"],
                      data=pl.DataFrame({"id": [key]}), delete_only=True)


def _vector_parts(lake, table: str) -> list:
    snapshot, _ = SimpleTable(
        super_table=SuperTable(lake.name, ORG), simple_name=table,
    ).get_simple_table_snapshot()
    pointer = snapshot.get("tombstone")
    return [pointer] if isinstance(pointer, str) else list(pointer or [])


def _on_the_connection():
    """(names of the vector tables on this thread's connection, the cache's entries)."""
    state = engine_duckdb._shared_state()
    names = {row[0] for row in state["con"].execute(
        "SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'dv_%'").fetchall()}
    return names, state["tombstone_cache"].snapshot()


@pytest.fixture
def lake(hermetic_fakeredis):
    engine_duckdb.reset_shared_duckdb_state()
    name = f"dvcap_{uuid.uuid4().hex[:10]}"
    SuperTable(name, ORG)
    writer = DataWriter(super_name=name, organization=ORG)
    for table in ("orders", "customers"):
        writer.write(role_name=ADMIN, simple_name=table, overwrite_columns=[],
                     data=pl.DataFrame({"id": list(range(ROWS)),
                                        "amount": [i * 10 for i in range(ROWS)]}))
    yield SimpleNamespace(name=name, writer=writer)
    engine_duckdb.reset_shared_duckdb_state()


def test_a_read_after_every_delete_keeps_the_cap_not_every_version(lake):
    cap = settings.SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_MAX_PER_TABLE
    deletes = cap + 6

    for done in range(1, deletes + 1):
        _delete(lake, "orders", done - 1)
        assert _count(lake.name, "orders") == ROWS - done      # every read is right

    # the case the cap missed: the vector is a list of parts, one per delete
    assert len(_vector_parts(lake, "orders")) == deletes

    names, entries = _on_the_connection()
    assert len(entries) == cap
    assert names == {entry["table_name"] for entry in entries}
    assert len({entry["table_id"] for entry in entries}) == 1
    assert all(entry["ref_count"] == 0 for entry in entries)
    # ... and a read still sees every delete
    assert _count(lake.name, "orders") == ROWS - deletes


def test_each_table_has_its_own_cap(lake):
    cap = settings.SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_MAX_PER_TABLE
    for done in range(1, cap + 4):                             # the busy table
        _delete(lake, "orders", done - 1)
        assert _count(lake.name, "orders") == ROWS - done
    for done in range(1, 3):                                   # the quiet one: two versions
        _delete(lake, "customers", done - 1)
        assert _count(lake.name, "customers") == ROWS - done

    names, entries = _on_the_connection()
    by_table = {}
    for entry in entries:
        by_table.setdefault(entry["table_id"], []).append(entry["table_name"])
    assert sorted(len(tables) for tables in by_table.values()) == [2, cap]
    assert len(names) == cap + 2
    assert _count(lake.name, "orders") == ROWS - (cap + 3)
    assert _count(lake.name, "customers") == ROWS - 2
