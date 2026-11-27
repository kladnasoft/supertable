"""
The versions of one table's deletion vector are one table to the cache.

``TombstoneCache`` keeps at most ``capacity`` versions of a table's vector
(``SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_MAX_PER_TABLE``, 8 by default).  Which
table a version belongs to is read from the cache key by ``dv_table_id``.

The key is what ``DataReader.execute`` makes it: the snapshot's ``tombstone``
pointer as text.  Since the vector became a list of parts that text is the text
of the whole list, and every delete adds a part.  ``dv_table_id`` used to take
everything before the key's last separator, which for a list holds every
earlier part: each version was a table of its own, the cap never applied, and a
table that was deleted from and read kept every version of its vector on every
reading thread until the idle time ran out (measured: 1,750 vector tables and
541 MB for one small table).

These tests use keys in the shape the reader makes (``str(pointer)``), paths in
the layout the writer makes (``hourly_partition_subpath``), and for the cache
itself real parquet parts on a real DuckDB connection.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from supertable.engine.engine_common import (
    TombstoneCache,
    dv_table_id,
    dv_table_name,
    init_connection,
)
from supertable.utils.helper import hourly_partition_subpath

TABLE_DIR = "kladna-soft/lake/tables/orders/tombstone"
NOON = datetime(2026, 11, 27, 12, 30, tzinfo=timezone.utc)


def _part(index: int, table_dir: str = TABLE_DIR, at: datetime = NOON) -> str:
    """A part's key, as ``processing._partitioned_new_path`` lays it out."""
    return os.path.join(
        table_dir, hourly_partition_subpath(at), f"{1795800000000 + index}_{index:016x}_deleted.parquet",
    )


def _key(pointer) -> str:
    """The cache key the reader makes of a snapshot's ``tombstone`` pointer."""
    return str(pointer)


# --------------------------------------------------------------------------
# Which table a key belongs to
# --------------------------------------------------------------------------

def test_every_version_of_a_vector_of_parts_is_the_same_table():
    parts = [_part(i) for i in range(12)]
    versions = [_key(parts[:count]) for count in range(1, len(parts) + 1)]

    assert len(set(versions)) == len(versions)                # twelve different keys
    assert {dv_table_id(key) for key in versions} == {TABLE_DIR}


def test_a_list_of_one_part_and_the_path_itself_are_the_same_table():
    part = _part(0)
    assert dv_table_id(_key([part])) == dv_table_id(_key(part)) == TABLE_DIR


def test_versions_group_across_an_hour_a_day_and_a_year():
    later = [NOON + timedelta(hours=1), NOON + timedelta(days=1), NOON + timedelta(days=400)]
    parts = [_part(0)] + [_part(i + 1, at=when) for i, when in enumerate(later)]
    # the folders really differ, or this would prove nothing
    assert len({os.path.dirname(part) for part in parts}) == len(parts)

    assert {dv_table_id(_key(parts[:count])) for count in range(1, len(parts) + 1)} == {TABLE_DIR}


def test_parts_of_the_flat_layout_group_with_the_hour_folders():
    """Parts written before the hour folders sit directly in ``tombstone/``."""
    flat = f"{TABLE_DIR}/1790000000000_aa_deleted.parquet"
    assert dv_table_id(_key(flat)) == TABLE_DIR
    assert dv_table_id(_key([flat])) == TABLE_DIR
    assert dv_table_id(_key([flat, _part(1)])) == TABLE_DIR
    assert dv_table_id(_key([_part(1), flat])) == TABLE_DIR


def test_the_hour_folders_are_the_ones_the_writer_makes():
    """Pins ``dv_table_id`` to ``hourly_partition_subpath``: a change of that
    layout has to be followed here, or versions group by the hour again."""
    for when in (NOON, datetime(2027, 1, 1, 0, 0, tzinfo=timezone.utc),
                 datetime(1999, 12, 31, 23, 59, tzinfo=timezone.utc)):
        path = os.path.join(TABLE_DIR, hourly_partition_subpath(when), "1_aa_deleted.parquet")
        assert dv_table_id(path) == TABLE_DIR


@pytest.mark.parametrize("other", [
    "kladna-soft/lake/tables/customers/tombstone",       # another table
    "kladna-soft/other_lake/tables/orders/tombstone",    # the same name in another lake
    "another-org/lake/tables/orders/tombstone",          # ... in another organization
])
def test_two_tables_are_two_tables(other):
    mine = _key([_part(0), _part(1)])
    theirs = _key([_part(0, table_dir=other), _part(1, table_dir=other)])
    assert dv_table_id(theirs) == other
    assert dv_table_id(mine) != dv_table_id(theirs)


def test_only_the_trailing_hour_folders_are_left_out():
    """A folder higher up that happens to look like one is part of the table."""
    table_dir = "org/year=2020/tables/hour=7/tombstone"
    assert dv_table_id(_key([_part(0, table_dir=table_dir), _part(1, table_dir=table_dir)])) == table_dir
    # ... and a look-alike is not an hour folder
    odd = "org/lake/tables/t/tombstone/yearly=3/1_aa_deleted.parquet"
    assert dv_table_id(odd) == "org/lake/tables/t/tombstone/yearly=3"


def test_backslash_separators_group_as_well():
    """A key made on Windows: ``repr`` doubles every backslash of a path."""
    table_dir = "C:\\lake\\tables\\orders\\tombstone"
    parts = [f"{table_dir}\\year=2026\\month=11\\day=27\\hour={12 + i // 2}\\{i}_aa_deleted.parquet"
             for i in range(4)]                               # two hours
    ids = {dv_table_id(_key(parts[:count])) for count in range(1, len(parts) + 1)}
    assert len(ids) == 1
    assert "hour=" not in ids.pop()
    assert dv_table_id(parts[0]) == table_dir                 # the path by itself


def test_keys_without_a_separator_still_group_alone():
    assert dv_table_id("bare") == "bare"
    assert dv_table_id(_key(["bare.parquet"])) == _key(["bare.parquet"])
    assert dv_table_id("/rooted.parquet") == "/rooted.parquet"


# --------------------------------------------------------------------------
# The cache itself, on real parts
# --------------------------------------------------------------------------

def _write_part(con, path: str, rowids) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    values = ",".join(f"({rowid})" for rowid in rowids)
    con.execute(
        f'COPY (SELECT * FROM (VALUES {values}) AS t("__rowid__")) '
        f"TO '{path}' (FORMAT PARQUET)"
    )


def _tables(con) -> set:
    return {row[0] for row in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'dv_%'").fetchall()}


@pytest.fixture
def vector(duckdb_con, tmp_path):
    """A vector of twelve parts, three row ids each, written where a lake would
    write them.  ``versions[i]`` is ``(key, paths)`` after ``i + 1`` deletes."""
    init_connection(duckdb_con, temp_dir=str(tmp_path / "spill"))
    keys = [_part(i) for i in range(12)]
    paths = [str(tmp_path / key) for key in keys]
    for index, path in enumerate(paths):
        _write_part(duckdb_con, path, [index * 3, index * 3 + 1, index * 3 + 2])
    return [(_key(keys[:count]), paths[:count]) for count in range(1, len(keys) + 1)]


def test_the_cap_holds_for_a_vector_that_gains_a_part_with_every_delete(duckdb_con, vector):
    cache = TombstoneCache(capacity=3, ttl_seconds=10_000, time_fn=lambda: 0.0)

    for key, paths in vector:                                  # a read after every delete
        assert cache.acquire(duckdb_con, key, paths) == dv_table_name(key)
        cache.release(duckdb_con, key)

    kept = cache.snapshot()
    assert [entry["cache_key"] for entry in kept] == [key for key, _ in vector[-3:]]
    assert {entry["table_id"] for entry in kept} == {TABLE_DIR}
    assert _tables(duckdb_con) == {dv_table_name(key) for key, _ in vector[-3:]}
    # what is kept is whole: a version of n parts holds the row ids of all n
    for key, paths in vector[-3:]:
        rows = duckdb_con.execute(
            f'SELECT count(*), min("__rowid__"), max("__rowid__") FROM {dv_table_name(key)}'
        ).fetchone()
        assert rows == (3 * len(paths), 0, 3 * len(paths) - 1)


def test_a_version_dropped_for_the_cap_is_built_again_when_asked_for(duckdb_con, vector):
    cache = TombstoneCache(capacity=2, ttl_seconds=10_000, time_fn=lambda: 0.0)
    for key, paths in vector[:5]:
        cache.acquire(duckdb_con, key, paths)
        cache.release(duckdb_con, key)
    first_key, first_paths = vector[0]
    assert dv_table_name(first_key) not in _tables(duckdb_con)

    name = cache.acquire(duckdb_con, first_key, first_paths)   # a reader of the old snapshot
    assert duckdb_con.execute(f"SELECT count(*) FROM {name}").fetchone()[0] == 3
    cache.release(duckdb_con, first_key)
    assert len(cache.snapshot()) == 2


def test_a_version_in_use_is_kept_over_the_cap(duckdb_con, vector):
    cache = TombstoneCache(capacity=2, ttl_seconds=10_000, time_fn=lambda: 0.0)
    open_key, open_paths = vector[0]
    cache.acquire(duckdb_con, open_key, open_paths)            # an open stream reads through it

    for key, paths in vector[1:8]:
        cache.acquire(duckdb_con, key, paths)
        cache.release(duckdb_con, key)

    kept = {entry["cache_key"]: entry["ref_count"] for entry in cache.snapshot()}
    # itself, in use, and the newest one: it counts towards the two
    assert kept == {open_key: 1, vector[7][0]: 0}
    assert dv_table_name(open_key) in _tables(duckdb_con)

    cache.release(duckdb_con, open_key)                        # the stream closes
    key, paths = vector[8]                                     # and the next delete is read
    cache.acquire(duckdb_con, key, paths)
    cache.release(duckdb_con, key)
    assert [entry["cache_key"] for entry in cache.snapshot()] == [vector[7][0], vector[8][0]]
    assert dv_table_name(open_key) not in _tables(duckdb_con)


def test_a_table_under_deletes_does_not_cost_another_table_its_vector(duckdb_con, vector, tmp_path):
    cache = TombstoneCache(capacity=2, ttl_seconds=10_000, time_fn=lambda: 0.0)
    quiet_dir = "kladna-soft/lake/tables/customers/tombstone"
    quiet_key = _part(0, table_dir=quiet_dir)
    quiet_path = str(tmp_path / quiet_key)
    _write_part(duckdb_con, quiet_path, [7])
    cache.acquire(duckdb_con, _key([quiet_key]), [quiet_path])
    cache.release(duckdb_con, _key([quiet_key]))

    for key, paths in vector:
        cache.acquire(duckdb_con, key, paths)
        cache.release(duckdb_con, key)

    by_table = {}
    for entry in cache.snapshot():
        by_table.setdefault(entry["table_id"], []).append(entry["cache_key"])
    assert by_table == {
        quiet_dir: [_key([quiet_key])],
        TABLE_DIR: [key for key, _ in vector[-2:]],
    }


def test_the_idle_time_still_empties_a_table_that_is_no_longer_read(duckdb_con, vector, tmp_path):
    clock = [1000.0]
    cache = TombstoneCache(capacity=8, ttl_seconds=300, time_fn=lambda: clock[0])
    for key, paths in vector[:4]:
        cache.acquire(duckdb_con, key, paths)
        cache.release(duckdb_con, key)
    assert len(cache.snapshot()) == 4

    clock[0] = 1301.0                                          # past the idle time
    other_key = _part(0, table_dir="kladna-soft/lake/tables/customers/tombstone")
    other_path = str(tmp_path / other_key)
    _write_part(duckdb_con, other_path, [1])
    cache.acquire(duckdb_con, _key([other_key]), [other_path])   # any read sweeps

    assert [entry["cache_key"] for entry in cache.snapshot()] == [_key([other_key])]
    assert _tables(duckdb_con) == {dv_table_name(_key([other_key]))}
