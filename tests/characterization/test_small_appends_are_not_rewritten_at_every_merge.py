"""
Small appends are not read back and rewritten at every merge.

A write merges the small files of its table once ``max_overlapping_files`` of
them have piled up.  The file that merge writes is usually small again, and
every later merge used to take it back in with the files that had arrived
since: each merge read and rewrote a bigger file than the last.  Over 1,400
appends of 5,000 rows every row was rewritten five times, merges grew from
0.4 s to 0.9 s and the writer process from 0.4 GB to 2.7 GB.

A merge now leaves out a file that would be more than a tenth of it.  The real
writer and reader run here over the hermetic fake Redis on LOCAL storage, with
the file limit at twenty so that merges come quickly.  What is checked is what
a merge reads and retires, and that the table still holds exactly the rows it
was given.

``supertable/tests/test_a_merge_leaves_out_files_already_merged.py`` is the
unit side of the selection.
"""

from __future__ import annotations

import logging
import uuid
from types import SimpleNamespace

import polars as pl
import pyarrow.parquet as pq
import pytest

from supertable import processing as processing_module
from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine.engine_enum import Engine
from supertable.processing import count_small_files
from supertable.redis_catalog import RedisCatalog
from supertable.simple_table import SimpleTable
from supertable.storage.local_storage import LocalStorage
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
TABLE = "events"
LIMIT = 20
LOGGER = "supertable.config.defaults"


def _batch(first: int, count: int = 3) -> pl.DataFrame:
    ids = list(range(first, first + count))
    return pl.DataFrame({"id": ids, "kind": [f"k{i % 4}" for i in ids],
                         "amount": [i * 10 for i in ids]})


@pytest.fixture
def lake(hermetic_fakeredis, monkeypatch):
    engine_duckdb.reset_shared_duckdb_state()
    sup = f"tiers_{uuid.uuid4().hex[:12]}"
    SuperTable(sup, ORG)
    writer = DataWriter(super_name=sup, organization=ORG)

    data_file_reads = []
    real_read_parquet = LocalStorage.read_parquet

    def read_parquet(self, path, *args, **kwargs):
        data_file_reads.append(path)
        return real_read_parquet(self, path, *args, **kwargs)

    monkeypatch.setattr(LocalStorage, "read_parquet", read_parquet)

    def snapshot() -> dict:
        leaf = RedisCatalog().get_leaf(ORG, sup, TABLE)
        return leaf["payload"] if leaf else {"resources": []}

    def write(frame: pl.DataFrame, overwrite_columns=(), **options) -> SimpleNamespace:
        """Write *frame*; say what the write retired, wrote and read."""
        before = snapshot()
        data_file_reads.clear()
        writer.write(role_name=ADMIN, simple_name=TABLE,
                     overwrite_columns=list(overwrite_columns), data=frame, **options)
        after = snapshot()
        old = {r["file"]: r for r in before["resources"]}
        new = {r["file"]: r for r in after["resources"]}
        retired = [r for path, r in old.items() if path not in new]
        written = [r for path, r in new.items() if path not in old]
        data_reads = [p for p in data_file_reads if "/data/" in p]
        return SimpleNamespace(before=before, after=after, retired=retired,
                               written=written, reads=data_reads)

    def rows(sql: str = f"SELECT id, kind, amount FROM {TABLE} ORDER BY id"):
        frame, status, message = DataReader(
            super_name=sup, organization=ORG, query=sql,
        ).execute(role_name=ADMIN, engine=Engine.DUCKDB)
        assert status is Status.OK, message
        return frame.to_dicts()

    write(_batch(0))                                   # the table exists from here on
    writer.configure_table(ADMIN, TABLE, max_overlapping_files=LIMIT)
    config = {"max_overlapping_files": LIMIT}

    yield SimpleNamespace(sup=sup, writer=writer, snapshot=snapshot, write=write,
                          rows=rows, config=config)
    engine_duckdb.reset_shared_duckdb_state()


def _expected(ids) -> list:
    return [{"id": i, "kind": f"k{i % 4}", "amount": i * 10} for i in sorted(ids)]


def _append_until_a_merge(lake, next_id: int):
    """Append batches of three rows until a write retires files. Returns that
    write and the next unused id."""
    for _ in range(LIMIT + 2):
        done = lake.write(_batch(next_id))
        next_id += 3
        if done.retired:
            return done, next_id
    raise AssertionError("no write merged")


# ---------------------------------------------------------------------------
# One merge, then the next
# ---------------------------------------------------------------------------

def test_the_first_merge_takes_every_file_as_before(lake):
    done, next_id = _append_until_a_merge(lake, 3)

    assert next_id == 3 * LIMIT, "the twentieth small file opens the gate"
    assert len(done.retired) == LIMIT - 1 and len(done.written) == 1
    assert len(done.after["resources"]) == 1
    assert done.written[0]["rows"] == 3 * LIMIT
    assert lake.rows() == _expected(range(3 * LIMIT))


def test_the_next_merge_leaves_the_merged_file_alone(lake):
    first, next_id = _append_until_a_merge(lake, 3)
    merged = first.written[0]

    second, next_id = _append_until_a_merge(lake, next_id)

    # The nineteen files written since, and not the file of sixty rows.
    assert len(second.retired) == LIMIT - 2, "eighteen older files and the incoming one"
    assert merged["file"] not in {r["file"] for r in second.retired}
    assert merged["file"] not in second.reads, "it is not even opened"
    assert sorted(r["rows"] for r in second.retired) == [3] * (LIMIT - 2)
    assert [r["rows"] for r in second.after["resources"]] == [3 * LIMIT, 3 * (LIMIT - 1)]
    assert merged in second.after["resources"], "the same entry, untouched"
    assert lake.rows() == _expected(range(next_id))


def test_the_write_says_how_many_small_files_it_left(lake, caplog):
    _first, next_id = _append_until_a_merge(lake, 3)

    with caplog.at_level(logging.INFO, logger=LOGGER):
        _append_until_a_merge(lake, next_id)

    lines = [r.getMessage() for r in caplog.records
             if "compaction during write" in r.getMessage()]
    assert len(lines) == 1, lines
    assert "small-file phase merged 19 small file(s) -> 1 file(s) (57 row(s)); " \
           "1 small file(s) left for a later merge; large files left untouched" in lines[0]
    assert "compaction io: read 19 file(s)" in lines[0]


def test_a_merge_that_leaves_nothing_out_says_nothing_about_it(lake, caplog):
    with caplog.at_level(logging.INFO, logger=LOGGER):
        _append_until_a_merge(lake, 3)

    lines = [r.getMessage() for r in caplog.records
             if "compaction during write" in r.getMessage()]
    assert len(lines) == 1, lines
    assert "left for a later merge" not in lines[0]
    assert "small-file phase merged 20 small file(s) -> 1 file(s) (60 row(s)); " \
           "large files left untouched" in lines[0]


# ---------------------------------------------------------------------------
# Many appends
# ---------------------------------------------------------------------------

def test_over_many_appends_a_row_is_rewritten_a_few_times(lake):
    """260 appends of three rows.  Taking every small file into every merge
    rewrote each row more than six times here; leaving merged files out, under
    two.  The number of small files a reader meets stays under the limit."""
    appended = 3
    rewritten = merges = 0
    left_out = 0
    for i in range(1, 260):
        done = lake.write(_batch(3 * i))
        appended += 3
        if done.retired:
            merges += 1
            # The incoming file was written and merged in one write: not a rewrite.
            rewritten += sum(r["rows"] for r in done.retired)
            left_out += sum(
                1 for r in done.before["resources"]
                if r in done.after["resources"] and r["rows"] > 3)
        small = count_small_files(done.after["resources"], lake.config)
        assert small < LIMIT, "the gate keeps the count of small files under the limit"

    assert merges >= 13
    assert left_out > 20, "merged files were left out of later merges"
    assert rewritten <= 2.0 * appended
    assert lake.rows(f"SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS d, SUM(amount) AS s "
                     f"FROM {TABLE}") == [
        {"n": appended, "d": appended, "s": sum(i * 10 for i in range(appended))}]
    assert lake.rows() == _expected(range(appended))


def test_taking_every_small_file_is_what_made_it_worse(lake, monkeypatch):
    """The same appends with the selection switched off, to show the test above
    measures the selection and not something else."""
    monkeypatch.setattr(
        "supertable.data_writer.select_small_files_to_merge",
        lambda resources, table_config=None: [
            r for r in resources
            if processing_module._is_small_file(
                r, processing_module._resolve_limits(table_config)[0])])
    appended, rewritten = 3, 0
    for i in range(1, 260):
        done = lake.write(_batch(3 * i))
        appended += 3
        rewritten += sum(r["rows"] for r in done.retired)

    assert rewritten > 5.0 * appended
    assert lake.rows() == _expected(range(appended))


# ---------------------------------------------------------------------------
# With upserts and deletes in between
# ---------------------------------------------------------------------------

def test_upserts_and_deletes_between_merges_leave_exactly_the_right_rows(lake):
    """Deleted rows are dropped from their files before any merge (the gate
    drains the deletion vector first), whichever files the merge then takes."""
    model = {i: i * 10 for i in range(3)}
    next_id = 3
    merges = left_out = 0
    for step in range(1, 130):
        if step % 7 == 0:
            # Replace an old row and a recent one (or put back one that was deleted).
            keys = sorted({step % next_id, next_id - 1})
            frame = pl.DataFrame({"id": keys, "kind": ["up"] * len(keys),
                                  "amount": [-(k + step) for k in keys]})
            done = lake.write(frame, overwrite_columns=["id"])
            for k in keys:
                model[k] = -(k + step)
        elif step % 11 == 0:
            victim = sorted(model)[step % len(model)]
            done = lake.write(pl.DataFrame({"id": [victim]}), overwrite_columns=["id"],
                              delete_only=True)
            del model[victim]
        else:
            done = lake.write(_batch(next_id))
            for i in range(next_id, next_id + 3):
                model[i] = i * 10
            next_id += 3
        if len(done.retired) > 2:
            merges += 1
            left_out += sum(1 for r in done.before["resources"]
                            if r in done.after["resources"] and r["rows"] > 3)
        assert count_small_files(done.after["resources"], lake.config) < LIMIT

    assert merges >= 5
    assert left_out >= 5, "merges that left files out were part of this"
    got = lake.rows(f"SELECT id, amount FROM {TABLE} ORDER BY id")
    assert got == [{"id": i, "amount": model[i]} for i in sorted(model)]


# ---------------------------------------------------------------------------
# Explicit compaction
# ---------------------------------------------------------------------------

def test_explicit_compaction_still_merges_every_small_file(lake):
    _first, next_id = _append_until_a_merge(lake, 3)
    _second, next_id = _append_until_a_merge(lake, next_id)
    for _ in range(4):
        lake.write(_batch(next_id))
        next_id += 3
    assert len(lake.snapshot()["resources"]) == 6, "two merged files and four new ones"

    result = lake.writer.compact(role_name=ADMIN, simple_name=TABLE)

    assert result["files_before"] == 6 and result["files_after"] == 1
    assert result["files_compacted"] == 6
    assert lake.snapshot()["resources"][0]["rows"] == next_id
    assert lake.rows() == _expected(range(next_id))


# ---------------------------------------------------------------------------
# Export goes through the same merge
# ---------------------------------------------------------------------------

def test_an_export_holds_every_live_row_once(lake, tmp_path):
    for i in range(1, 6):
        lake.write(_batch(3 * i))
    lake.write(pl.DataFrame({"id": [4, 10]}), overwrite_columns=["id"], delete_only=True)
    assert lake.snapshot()["tombstone_rows"] == 2, "deleted, not yet dropped from the files"

    result = SimpleTable(SuperTable(lake.sup, ORG), TABLE).export_to(str(tmp_path / "export"))

    exported = []
    for path in result["files"]:
        exported.extend(pq.ParquetFile(path).read().column("id").to_pylist())
    live = [i for i in range(18) if i not in (4, 10)]
    assert sorted(exported) == live
    assert result["total_rows"] == len(live)
    assert result["files_written"] == len(result["files"]) == 1
    assert lake.rows() == _expected(live), "and the table itself is as it was"
