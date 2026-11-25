"""
A write without overwrite columns asks the overlap step for no candidates.

``find_overlapping_files`` built, for every write, a set of every small file
of the table.  ``DataWriter.write`` reads that set only when there are
overwrite columns to match on, so an append and a delete of every row had it
built to throw it away.  They now ask for the step's counters alone
(``want_candidates=False``); an upsert and a keyed delete get every file as a
candidate, as before.

The real writer and reader run here over the hermetic fake Redis on LOCAL
storage.  ``supertable/tests/test_the_overlap_step_returns_what_it_always_did.py``
compares the step itself with the step as it was.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import polars as pl
import pytest

from supertable import data_writer as data_writer_module
from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine.engine_enum import Engine
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
TABLE = "orders"


def _rows(first: int, count: int = 3) -> pl.DataFrame:
    ids = list(range(first, first + count))
    return pl.DataFrame({"id": ids, "region": ["eu" if i % 2 else "us" for i in ids],
                         "amount": [i * 10 for i in ids]})


@pytest.fixture
def lake(hermetic_fakeredis):
    engine_duckdb.reset_shared_duckdb_state()
    sup = f"overlap_{uuid.uuid4().hex[:12]}"
    SuperTable(sup, ORG)
    writer = DataWriter(super_name=sup, organization=ORG)

    def write(frame: pl.DataFrame, overwrite_columns=(), **options):
        return writer.write(role_name=ADMIN, simple_name=TABLE,
                            overwrite_columns=list(overwrite_columns), data=frame, **options)

    def read() -> list:
        frame, status, message = DataReader(
            super_name=sup, organization=ORG,
            query=f"SELECT id, amount FROM {TABLE} ORDER BY id",
        ).execute(role_name=ADMIN, engine=Engine.DUCKDB)
        assert status is Status.OK, message
        return [(row["id"], row["amount"]) for row in frame.to_dicts()]

    yield SimpleNamespace(write=write, read=read)
    engine_duckdb.reset_shared_duckdb_state()


def test_only_a_write_with_overwrite_columns_asks_for_the_overlap_candidates(lake, monkeypatch):
    """An append and a delete of every row match no key: they ask the step for
    its counters and get no set built.  An upsert and a keyed delete get every
    file, as before.  The table reads back what those writes leave."""
    asked = []
    real = data_writer_module.find_overlapping_files

    def recording(*args, **options):
        candidates = real(*args, **options)
        asked.append((options["want_candidates"], len(candidates),
                      options["profiler"].counts.get("resources_total", 0)))
        return candidates

    monkeypatch.setattr(data_writer_module, "find_overlapping_files", recording)

    lake.write(_rows(0))
    lake.write(_rows(3))
    assert lake.read() == [(0, 0), (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]
    lake.write(pl.DataFrame({"id": [1, 4], "region": ["eu", "us"], "amount": [-1, -4]}),
               overwrite_columns=["id"])
    lake.write(pl.DataFrame({"id": [0]}), overwrite_columns=["id"], delete_only=True)
    assert lake.read() == [(1, -1), (2, 20), (3, 30), (4, -4), (5, 50)]
    lake.write(pl.DataFrame({"id": [0]}), overwrite_columns=[], delete_only=True)

    assert [wanted for wanted, _n, _total in asked] == [False, False, True, True, False]
    assert [n for _wanted, n, _total in asked] == [0, 0, 2, 3, 0]
    assert [total for _wanted, _n, total in asked] == [0, 1, 2, 3, 3]
    assert lake.read() == []
