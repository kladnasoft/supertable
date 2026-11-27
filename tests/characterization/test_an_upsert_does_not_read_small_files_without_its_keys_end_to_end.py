"""
An upsert does not read the small files that hold none of its keys.

Every small upsert leaves a small file whose few keys are spread over the key
range, so the stored minimum and maximum cannot rule it out, and every later
upsert or delete read it.  The writing process now keeps the distinct key
values of small files, from the frame it wrote a file from or read it into,
and drops a file from the candidates when none of its values is an incoming
one (``supertable.processing.drop_files_without_incoming_keys``).

The real writer and reader run here over the hermetic fake Redis on LOCAL
storage, and every data file a write reads is recorded.  The same writes are
run twice, with the kept values and with ``SUPERTABLE_KEY_VALUES_CACHE_MAX_BYTES``
at zero, which is the writer as it was: they must leave the same rows, which
must be the rows a plain model of the table says, and the first must read no
file the second does not.

``supertable/tests/test_small_files_are_ruled_out_by_their_key_values.py`` is
the unit side: which files are dropped, checked against the join itself.
"""

from __future__ import annotations

import dataclasses
import random
import uuid
from types import SimpleNamespace

import polars as pl
import pytest

from supertable import processing as processing_module
from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine.engine_enum import Engine
from supertable.redis_catalog import RedisCatalog
from supertable.storage.local_storage import LocalStorage
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
TABLE = "orders"
KNOWN = processing_module._FILE_KEY_VALUES


def _rows(ids, amount) -> pl.DataFrame:
    ids = list(ids)
    return pl.DataFrame({"id": ids, "amount": [amount] * len(ids)},
                        schema={"id": pl.Int64, "amount": pl.Int64})


@pytest.fixture
def lakes(hermetic_fakeredis, monkeypatch):
    """``lakes(keep)`` opens a new lake whose writer keeps key values, or does not."""
    engine_duckdb.reset_shared_duckdb_state()
    KNOWN.clear()
    reads: list = []
    real_read_parquet = LocalStorage.read_parquet

    def read_parquet(self, path, *args, **kwargs):
        reads.append(path)
        return real_read_parquet(self, path, *args, **kwargs)

    monkeypatch.setattr(LocalStorage, "read_parquet", read_parquet)

    def open_lake(keep: bool = True, key_type=pl.Int64) -> SimpleNamespace:
        sup = f"keys_{uuid.uuid4().hex[:12]}"
        SuperTable(sup, ORG)
        writer = DataWriter(super_name=sup, organization=ORG)
        bound = processing_module._key_values_cache_cap if keep else (lambda: 0)

        def snapshot() -> dict:
            leaf = RedisCatalog().get_leaf(ORG, sup, TABLE)
            return leaf["payload"] if leaf else {"resources": []}

        def write(frame: pl.DataFrame, overwrite_columns=("id",), **options) -> list:
            """Write *frame*; the names of the data files the write read, in order."""
            monkeypatch.setattr(KNOWN, "_cap_getter", bound)
            reads.clear()
            writer.write(role_name=ADMIN, simple_name=TABLE,
                         overwrite_columns=list(overwrite_columns), data=frame, **options)
            return [p for p in reads if "/data/" in p]

        def delete(ids) -> list:
            return write(pl.DataFrame({"id": list(ids)}, schema={"id": key_type}),
                         delete_only=True)

        def files() -> list:
            return [r["file"] for r in snapshot()["resources"]]

        def rows() -> list:
            frame, status, message = DataReader(
                super_name=sup, organization=ORG,
                query=f"SELECT id, amount FROM {TABLE} ORDER BY id, amount",
            ).execute(role_name=ADMIN, engine=Engine.DUCKDB)
            assert status is Status.OK, message
            return [(row["id"], row["amount"]) for row in frame.to_dicts()]

        return SimpleNamespace(sup=sup, writer=writer, write=write, delete=delete,
                               files=files, rows=rows, snapshot=snapshot)

    yield open_lake
    KNOWN.clear()
    engine_duckdb.reset_shared_duckdb_state()


def _new_file(lake, before: list) -> str:
    (new,) = [f for f in lake.files() if f not in before]
    return new


def _by_id(rows: list) -> dict:
    """``id -> amount``, and no id twice: a row that was replaced and is still
    there is what a wrongly dropped file leaves behind."""
    ids = [i for i, _ in rows]
    assert len(ids) == len(set(ids)), sorted(i for i in set(ids) if ids.count(i) > 1)
    return dict(rows)


# ---------------------------------------------------------------------------
# Which files an upsert reads
# ---------------------------------------------------------------------------

def test_an_upsert_reads_the_files_that_hold_its_keys(lakes):
    lake = lakes(keep=True)
    lake.write(_rows(range(50), 0), overwrite_columns=())          # an append: nothing is kept of it
    (base,) = lake.files()
    assert KNOWN.stats() == (0, 0)

    before = lake.files()
    assert lake.write(_rows([3, 47], 1)) == [base]                 # read, and known from here on
    first = _new_file(lake, before)

    before = lake.files()
    assert lake.write(_rows([10, 40], 2)) == [base]                # 10..40 is inside 3..47: not read
    second = _new_file(lake, before)

    before = lake.files()
    assert sorted(lake.write(_rows([47, 20], 3))) == sorted([base, first])
    third = _new_file(lake, before)

    assert lake.write(_rows([60, 61], 4)) == []                    # beyond every range, as before
    assert sorted(lake.write(_rows([20, 21, 22], 5))) == sorted([base, third])
    assert lake.write(_rows([0, 49], 6)) == [base]

    expected = {i: 0 for i in range(50)}
    expected.update({3: 1, 47: 3, 10: 2, 40: 2, 20: 5, 21: 5, 22: 5, 60: 4, 61: 4, 0: 6, 49: 6})
    assert lake.rows() == sorted(expected.items())
    assert second in lake.files()


def test_without_the_kept_values_every_file_in_range_is_read(lakes):
    """The writer as it was: ``SUPERTABLE_KEY_VALUES_CACHE_MAX_BYTES=0``."""
    lake = lakes(keep=False)
    lake.write(_rows(range(50), 0), overwrite_columns=())
    (base,) = lake.files()
    assert lake.write(_rows([3, 47], 1)) == [base]
    assert len(lake.write(_rows([10, 40], 2))) == 2
    assert len(lake.write(_rows([47, 20], 3))) == 3
    assert KNOWN.stats() == (0, 0)


def test_a_delete_reads_the_small_file_that_holds_its_key_and_removes_the_row(lakes):
    lake = lakes(keep=True)
    lake.write(_rows(range(50), 0), overwrite_columns=())
    (base,) = lake.files()
    before = lake.files()
    lake.write(_rows([3, 47], 1))
    first = _new_file(lake, before)
    lake.write(_rows([10, 40], 2))
    assert len(lake.files()) == 3

    assert sorted(lake.delete([47])) == sorted([base, first])      # 47 is in both, dead in the base
    assert lake.delete([25]) == [base]
    assert len(lake.delete([3, 10])) == 3                           # 3 and 10: the base and both small files
    # a delete writes no file, and the small file whose two rows are now both
    # deleted is retired, as a file without a live row always was
    assert first not in lake.files() and len(lake.files()) == 2
    rows = _by_id(lake.rows())
    assert 47 not in rows and 25 not in rows and 3 not in rows and 10 not in rows
    assert rows[40] == 2 and len(rows) == 46


def test_a_newer_than_upsert_is_filtered_by_the_small_file_that_holds_its_key(lakes):
    lake = lakes(keep=True)
    lake.write(_rows(range(50), 5), overwrite_columns=())
    (base,) = lake.files()
    before = lake.files()
    lake.write(_rows([3, 47], 9))                                   # amount 9 is the newest of 3 and 47
    first = _new_file(lake, before)
    lake.write(_rows([10, 40], 7))

    # 47 at 8 is older than the 9 in the small file; 20 at 8 is newer than the 5 in the base
    stale_and_fresh = pl.DataFrame({"id": [47, 20], "amount": [8, 8]},
                                   schema={"id": pl.Int64, "amount": pl.Int64})
    assert sorted(lake.write(stale_and_fresh, newer_than="amount")) == sorted([base, first])
    rows = _by_id(lake.rows())
    assert rows[47] == 9 and rows[20] == 8 and len(rows) == 50


def test_a_process_that_has_not_met_the_files_reads_them_once(lakes):
    lake = lakes(keep=True)
    lake.write(_rows(range(50), 0), overwrite_columns=())
    for n, ids in enumerate(([3, 47], [10, 40], [5, 30])):
        lake.write(_rows(ids, n + 1))
    assert len(lake.files()) == 4

    KNOWN.clear()                                                   # what another process knows
    assert len(lake.write(_rows([11, 39], 8))) == 4                 # every file in range, once
    assert len(lake.write(_rows([12, 38], 9))) == 1                 # and then the base only
    rows = _by_id(lake.rows())
    assert rows[11] == 8 and rows[12] == 9 and rows[38] == 9 and len(rows) == 50


def test_string_keys(lakes):
    lake = lakes(keep=True, key_type=pl.Utf8)

    def named(names, amount) -> pl.DataFrame:
        return pl.DataFrame({"id": list(names), "amount": [amount] * len(names)},
                            schema={"id": pl.Utf8, "amount": pl.Int64})

    lake.write(named([f"k{i:03d}" for i in range(50)], 0), overwrite_columns=())
    (base,) = lake.files()
    before = lake.files()
    assert lake.write(named(["k003", "k047"], 1)) == [base]
    first = _new_file(lake, before)
    assert lake.write(named(["k010", "k040"], 2)) == [base]
    assert sorted(lake.write(named(["k047", "K047", "k04"], 3))) == sorted([base, first])
    rows = _by_id(lake.rows())
    assert rows["k047"] == 3 and rows["K047"] == 3 and rows["k04"] == 3 and rows["k003"] == 1
    assert len(rows) == 52


# ---------------------------------------------------------------------------
# The same writes, with and without
# ---------------------------------------------------------------------------

def _workload(seed: int, operations: int):
    """Random upserts, deletes, watermarked upserts and appends, and the rows
    they must leave: ``id -> amount``."""
    rng = random.Random(seed)
    model = {i: 0 for i in range(120)}
    steps = [("append", list(range(120)), 0)]
    next_new = 1000
    for n in range(1, operations + 1):
        kind = rng.choices(["upsert", "delete", "newer_than", "append"], [6, 2, 2, 1])[0]
        if kind == "append":
            ids = list(range(next_new, next_new + 3))
            next_new += 3
            for i in ids:
                model[i] = n
        elif kind == "delete":
            ids = rng.sample(range(160), rng.randint(1, 3))
            for i in ids:
                model.pop(i, None)
        elif kind == "upsert":
            ids = rng.sample(range(160), rng.randint(1, 5))
            for i in ids:
                model[i] = n
        else:
            ids = rng.sample(range(160), rng.randint(1, 4))
            # half the time older than what is there: amounts only ever grow
            amount = n if rng.random() < 0.5 else 0
            for i in ids:
                if i not in model or amount > model[i]:
                    model[i] = amount
            steps.append((kind, ids, amount))
            continue
        steps.append((kind, ids, n))
    return steps, model


def _run(lake, steps) -> list:
    """Run the steps; how many data files each of them read."""
    read = []
    for kind, ids, amount in steps:
        if kind == "append":
            read.append(len(lake.write(_rows(ids, amount), overwrite_columns=())))
        elif kind == "delete":
            read.append(len(lake.delete(ids)))
        elif kind == "newer_than":
            read.append(len(lake.write(_rows(ids, amount), newer_than="amount")))
        else:
            read.append(len(lake.write(_rows(ids, amount))))
    return read


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_the_same_writes_leave_the_same_rows_and_read_fewer_files(lakes, seed):
    steps, model = _workload(seed, operations=70)
    kept, as_it_was = lakes(keep=True), lakes(keep=False)

    read_kept = _run(kept, steps)
    read_as_it_was = _run(as_it_was, steps)

    assert kept.rows() == sorted(model.items())
    assert as_it_was.rows() == sorted(model.items())
    assert all(a <= b for a, b in zip(read_kept, read_as_it_was)), (read_kept, read_as_it_was)
    assert sum(read_kept) * 2 < sum(read_as_it_was), (sum(read_kept), sum(read_as_it_was))
    assert kept.snapshot()["tombstone_rows"] == as_it_was.snapshot()["tombstone_rows"]
    assert len(kept.files()) == len(as_it_was.files())


def test_the_same_writes_with_the_probe_on(lakes, monkeypatch):
    """``SUPERTABLE_DUCKDB_WRITE_PROBE=true``: one DuckDB scan finds the matching
    rows, over the files that are left.  With the scan no file is read into a
    frame, so what is checked is the table."""
    from supertable.config.settings import settings

    monkeypatch.setattr(processing_module, "settings",
                        dataclasses.replace(settings, SUPERTABLE_DUCKDB_WRITE_PROBE=True))
    steps, model = _workload(11, operations=60)
    kept, as_it_was = lakes(keep=True), lakes(keep=False)
    assert sum(_run(kept, steps)) == 0 and sum(_run(as_it_was, steps)) == 0
    assert kept.rows() == sorted(model.items())
    assert as_it_was.rows() == sorted(model.items())
    assert KNOWN.stats()[0] > 20                                   # the files this process wrote
    assert kept.snapshot()["tombstone_rows"] == as_it_was.snapshot()["tombstone_rows"]


def test_the_same_writes_through_merges(lakes):
    """With the file limit at twenty, small files are merged again and again."""
    steps, model = _workload(7, operations=90)
    kept, as_it_was = lakes(keep=True), lakes(keep=False)
    read = {}
    for name, lake in (("kept", kept), ("as it was", as_it_was)):
        first, rest = steps[:1], steps[1:]
        _run(lake, first)                                          # the table exists from here on
        lake.writer.configure_table(ADMIN, TABLE, max_overlapping_files=20)
        read[name] = _run(lake, rest)
        assert len(lake.files()) < 40                              # about 75 without a merge
    assert kept.rows() == as_it_was.rows() == sorted(model.items())
    assert sum(read["kept"]) < sum(read["as it was"])
