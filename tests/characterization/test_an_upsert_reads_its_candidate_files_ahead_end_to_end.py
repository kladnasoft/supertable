"""
An upsert or delete reads its candidate files ahead of their turn.

A write with overwrite columns reads every data file it could not rule out
and uses them in the order given.  The files after the one in use are now
read ahead by a few threads the process keeps
(``supertable.processing._CandidateReads``,
``SUPERTABLE_WRITE_READ_AHEAD_THREADS``).

The real writer and reader run here over the hermetic fake Redis on LOCAL
storage, and every data file a write reads is recorded with the thread that
read it.  The same writes are run one file after another, which is the writer
as it was, and several files at a time over a disk that answers late and out
of order: they must leave the same rows, which must be the rows a plain model
of the table says, and read the same number of files, each once.

``supertable/tests/test_candidate_files_are_read_ahead_of_their_turn.py`` is
the unit side: whose frame a file gets, how many at a time, what is raised.
"""

from __future__ import annotations

import dataclasses
import random
import threading
import time
import uuid
from types import SimpleNamespace

import polars as pl
import pytest

from supertable import processing as processing_module
from supertable.config.settings import settings
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
    """``lakes(threads)`` opens a new lake whose writer reads that many candidate files at a time.

    ``known=False`` (the default here) switches the kept key values off, so
    that every file the key range cannot rule out is read by every write.
    """
    engine_duckdb.reset_shared_duckdb_state()
    KNOWN.clear()
    reads: list = []
    lock = threading.Lock()
    real_read_parquet = LocalStorage.read_parquet

    def read_parquet(self, path, *args, **kwargs):
        with lock:
            reads.append((path, threading.current_thread().name))
        if "/data/" in path:
            # late, and not in the order asked: a later file is often back first
            time.sleep(random.Random(path).random() * 0.002)
        return real_read_parquet(self, path, *args, **kwargs)

    monkeypatch.setattr(LocalStorage, "read_parquet", read_parquet)

    def open_lake(threads: int, known: bool = False) -> SimpleNamespace:
        sup = f"ahead_{uuid.uuid4().hex[:12]}"
        SuperTable(sup, ORG)
        writer = DataWriter(super_name=sup, organization=ORG)
        mine = dataclasses.replace(
            settings,
            SUPERTABLE_WRITE_READ_AHEAD_THREADS=threads,
            SUPERTABLE_KEY_VALUES_CACHE_MAX_BYTES=(
                settings.SUPERTABLE_KEY_VALUES_CACHE_MAX_BYTES if known else 0),
        )

        def snapshot() -> dict:
            leaf = RedisCatalog().get_leaf(ORG, sup, TABLE)
            return leaf["payload"] if leaf else {"resources": []}

        def write(frame: pl.DataFrame, overwrite_columns=("id",), **options) -> list:
            """Write *frame*; ``(file, thread)`` of every data file the write read."""
            monkeypatch.setattr(processing_module, "settings", mine)
            with lock:
                reads.clear()
            writer.write(role_name=ADMIN, simple_name=TABLE,
                         overwrite_columns=list(overwrite_columns), data=frame, **options)
            with lock:
                return [(p, t) for p, t in reads if "/data/" in p]

        def delete(ids) -> list:
            return write(pl.DataFrame({"id": list(ids)}, schema={"id": pl.Int64}), delete_only=True)

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


def _by_id(rows: list) -> dict:
    """``id -> amount``, and no id twice: a row that was replaced and is still
    there is what a frame used for the wrong file leaves behind."""
    ids = [i for i, _ in rows]
    assert len(ids) == len(set(ids)), sorted(i for i in set(ids) if ids.count(i) > 1)
    return dict(rows)


def _scattered_table(lake, small_files: int = 12):
    """One file of 200 rows and *small_files* upserts of keys spread over it."""
    lake.write(_rows(range(200), 0), overwrite_columns=())
    expected = {i: 0 for i in range(200)}
    for n in range(1, small_files + 1):
        ids = [(n * 37 + j * 53) % 200 for j in range(4)]
        lake.write(_rows(ids, n))
        expected.update({i: n for i in ids})
    return expected


# ---------------------------------------------------------------------------
# Which files a write reads, and who reads them
# ---------------------------------------------------------------------------

def test_one_thread_reads_every_candidate_file_itself(lakes):
    lake = lakes(threads=1)
    _scattered_table(lake)
    read = lake.write(_rows([5, 150], 99))
    assert sorted(p for p, _ in read) == sorted(lake.files()[:-1])      # every file but the new one
    assert {t for _, t in read} == {threading.current_thread().name}


def test_several_threads_read_the_same_files_each_once(lakes):
    lake = lakes(threads=4)
    expected = _scattered_table(lake)
    before = lake.files()
    read = lake.write(_rows([5, 150], 99))
    expected.update({5: 99, 150: 99})

    assert sorted(p for p, _ in read) == sorted(before)                 # each once, none else
    readers = {t for _, t in read}
    assert any(t.startswith("supertable-read") for t in readers)        # some of them ahead of their turn
    assert _by_id(lake.rows()) == expected


def test_a_delete_reads_ahead_and_removes_the_rows(lakes):
    lake = lakes(threads=8)
    expected = _scattered_table(lake)
    before = lake.files()
    read = lake.delete([5, 150, 199])
    for i in (5, 150, 199):
        expected.pop(i)
    assert sorted(p for p, _ in read) == sorted(before)
    assert _by_id(lake.rows()) == expected


def test_a_newer_than_upsert_reads_ahead_and_keeps_the_newer_rows(lakes):
    lake = lakes(threads=8)
    expected = _scattered_table(lake)
    before = lake.files()
    ids = [5, 150, 37, 500]
    read = lake.write(_rows(ids, 6), newer_than="amount")               # 6: newer than some, older than others
    for i in ids:
        if i not in expected or 6 > expected[i]:
            expected[i] = 6
    # the stale filter reads every candidate once, ahead of their turn; the matching finds them read
    assert sorted(p for p, _ in read) == sorted(before)
    assert any(t.startswith("supertable-read") for _, t in read)
    assert _by_id(lake.rows()) == expected


def test_the_setting_as_it_ships_on_a_table_as_it_ships(lakes, monkeypatch):
    """Nothing set: the files are chosen by the storage, and the kept key values are on."""
    lake = lakes(threads=settings.SUPERTABLE_WRITE_READ_AHEAD_THREADS, known=True)
    expected = _scattered_table(lake, small_files=10)
    KNOWN.clear()                                                        # a process that has not met the files
    before = lake.files()
    read = lake.write(_rows([5, 150], 99))
    expected.update({5: 99, 150: 99})
    assert sorted(p for p, _ in read) == sorted(before)
    assert _by_id(lake.rows()) == expected


# ---------------------------------------------------------------------------
# The same writes, one file after another and several at a time
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
            amount = n if rng.random() < 0.5 else 0      # half the time older than what is there
            for i in ids:
                if i not in model or amount > model[i]:
                    model[i] = amount
            steps.append((kind, ids, amount))
            continue
        steps.append((kind, ids, n))
    return steps, model


def _run(lake, steps, once: bool = True) -> list:
    """Run the steps; how many data files each of them read.

    *once*: no write reads a file twice.  A write that also merges small
    files reads them a second time for the merge, as it always did.
    """
    read = []
    for kind, ids, amount in steps:
        if kind == "append":
            got = lake.write(_rows(ids, amount), overwrite_columns=())
        elif kind == "delete":
            got = lake.delete(ids)
        elif kind == "newer_than":
            got = lake.write(_rows(ids, amount), newer_than="amount")
        else:
            got = lake.write(_rows(ids, amount))
        paths = [p for p, _ in got]
        if once:
            assert len(paths) == len(set(paths)), "a file was read twice by one write"
        read.append(len(paths))
    return read


@pytest.mark.parametrize("seed", [1, 2, 3])
@pytest.mark.parametrize("threads", [2, 8])
def test_the_same_writes_leave_the_same_rows_and_read_the_same_files(lakes, seed, threads):
    steps, model = _workload(seed, operations=50)
    one_after_another, ahead = lakes(threads=1), lakes(threads=threads)

    read_one = _run(one_after_another, steps)
    read_ahead = _run(ahead, steps)

    assert _by_id(ahead.rows()) == model
    assert _by_id(one_after_another.rows()) == model
    assert read_ahead == read_one                                        # write by write
    assert sum(read_one) > 300                                           # and they did read
    assert ahead.snapshot()["tombstone_rows"] == one_after_another.snapshot()["tombstone_rows"]
    assert len(ahead.files()) == len(one_after_another.files())


def test_the_same_writes_through_merges(lakes):
    """With the file limit at twenty, small files are merged again and again."""
    steps, model = _workload(7, operations=70)
    one_after_another, ahead = lakes(threads=1), lakes(threads=8)
    for lake in (one_after_another, ahead):
        first, rest = steps[:1], steps[1:]
        _run(lake, first)                                                # the table exists from here on
        lake.writer.configure_table(ADMIN, TABLE, max_overlapping_files=20)
        _run(lake, rest, once=False)
        assert len(lake.files()) < 40
    assert _by_id(ahead.rows()) == _by_id(one_after_another.rows()) == model


def test_the_same_writes_with_the_kept_key_values(lakes):
    """The library as it ships: small files this process knows are not read at all."""
    steps, model = _workload(5, operations=50)
    one_after_another, ahead = lakes(threads=1, known=True), lakes(threads=8, known=True)
    read_one = _run(one_after_another, steps)
    read_ahead = _run(ahead, steps)
    assert _by_id(ahead.rows()) == _by_id(one_after_another.rows()) == model
    assert read_ahead == read_one


def test_writers_on_several_tables_at_once(lakes):
    """Four writers, each on a lake of its own, sharing the reading threads."""
    workloads = [_workload(20 + w, operations=25) for w in range(4)]
    mine = [lakes(threads=8) for _ in range(4)]
    failed = []

    def writer(w):
        try:
            for kind, ids, amount in workloads[w][0]:
                frame = _rows(ids, amount)
                options = {"overwrite_columns": ["id"]}
                if kind == "append":
                    options["overwrite_columns"] = []
                elif kind == "delete":
                    frame, options["delete_only"] = pl.DataFrame({"id": ids}, schema={"id": pl.Int64}), True
                elif kind == "newer_than":
                    options["newer_than"] = "amount"
                mine[w].writer.write(role_name=ADMIN, simple_name=TABLE, data=frame, **options)
        except BaseException as e:                                       # noqa: BLE001 - reported below
            failed.append((w, repr(e)))

    mine[0].write(_rows([10_000], 0), overwrite_columns=())              # the settings are in place
    ts = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(120)
    assert failed == [] and not any(t.is_alive() for t in ts)
    for w in range(4):
        expected = dict(workloads[w][1])
        if w == 0:
            expected[10_000] = 0
        assert _by_id(mine[w].rows()) == expected
