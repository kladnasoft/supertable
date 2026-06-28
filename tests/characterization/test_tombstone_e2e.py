"""End-to-end deletion-vector (tombstone) contract for the demo write sequence.

Unlike the sealed golden suite (which replays a *fixed* on-disk catalog through
the read path only), this test drives the **real** ``DataWriter`` to write the
seven demo fixtures in the demo order ``[1, 2, 6, 7, 3, 4, 5]`` for N passes,
then reconciles the engine's actual end-state against a hand-computed expectation
of what must be in the tombstone file, the stats, the snapshot, and the data.

Why this guards the merge-on-read deletion model:

  * H2 — a write must tombstone only rows that are still *live*; rows already in
    the deletion-vector must never be re-counted (otherwise ``deleted`` inflates
    and unchanged writes needlessly rewrite the vector).
  * H1 — a data file whose every physical row is tombstoned must be reclaimed
    (dropped from the snapshot, its rowids removed from the vector) rather than
    lingering until the compaction threshold.

Together they make the system converge to a fixed steady state: from the end of
the very first pass onward, every pass leaves an identical snapshot footprint
(7 files / 35 stats rows / 9 tombstoned rows / 26 live rows) and an identical
live result set, regardless of how many times the same data is rewritten.

Compaction is left at its (high) defaults so the demo never trips the compaction
threshold; reclamation here is pure metadata, never a data rewrite.
"""

from __future__ import annotations

from typing import List, Tuple

import polars as pl
import pytest

from supertable.data_reader import DataReader, engine
from supertable.data_writer import DataWriter
from supertable.demo.quickstart.dummy_data import get_dummy_data
from supertable.simple_table import SimpleTable
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "demo"
SIMPLE = "facts"
ROLE = "superadmin"
OVERWRITE = ["day", "client"]
WRITE_ORDER = [1, 2, 6, 7, 3, 4, 5]

# Per-write ``deleted`` counts.  The first pass writes onto an empty table, so
# only the overlapping fixtures (3, 4, 5) delete anything; every later pass
# overwrites a fully-populated table, so each fixture deletes the live rows for
# its keys.
EXPECTED_DELETED_FIRST_PASS = [0, 0, 0, 0, 1, 4, 4]
EXPECTED_DELETED_LATER_PASS = [5, 6, 5, 5, 4, 6, 4]

# Steady-state snapshot footprint at the end of any pass (N >= 1).
EXPECTED_FILES = 7
EXPECTED_SUM_ROWS = 35          # physical rows across all live files
EXPECTED_STATS_ROWS = 35       # one stats row per (file x row_group x data column)
EXPECTED_TOMBSTONE_ROWS = 9    # dead-but-not-yet-compacted physical rows
# Dead rows are concentrated in exactly four files; the rest are fully live.
EXPECTED_DEAD_FILE_COUNT = 4
EXPECTED_DEAD_COUNTS_SORTED = [2, 2, 2, 3]

# The 26 surviving rows, keyed by (day, client), after the demo sequence.
# ``value`` is None for fixture 7 (which drops the column).
EXPECTED_LIVE_ROWS: List[Tuple[str, str, object, str]] = [
    ("2023-01-01", "client1", 10, "Second partition"),
    ("2023-01-02", "client1", 20, "Second partition"),
    ("2023-01-03", "client1", 30, "First partition"),
    ("2023-01-04", "client1", 40, "First partition"),
    ("2023-01-05", "client1", 30, "Second partition"),
    ("2023-01-05", "client1", 40, "Second partition"),
    ("2023-01-05", "client1", 50, "Second partition"),
    ("2023-01-05", "client2", 50, "First partition"),
    ("2023-01-06", "client1", 30, "First partition"),
    ("2023-01-06", "client1", 40, "First partition"),
    ("2023-01-06", "client1", 50, "First partition"),
    ("2023-01-07", "client1", 20, "First partition"),
    ("2023-01-07", "client1", 30, "First partition"),
    ("2023-01-08", "client1", 30, "First partition"),
    ("2023-01-10", "client1", 40, "First partition"),
    ("2023-01-11", "client1", 50, "First partition"),
    ("2023-01-12", "client1", 60, "First partition"),
    ("2023-01-13", "client1", 70, "First partition"),
    ("2023-01-14", "client1", 80, "First partition"),
    ("2023-01-15", "client1", 90, "First partition"),
    ("2023-01-16", "client1", 100, "First partition"),
    ("2023-01-17", "client1", None, "First partition"),
    ("2023-01-18", "client1", None, "First partition"),
    ("2023-01-19", "client1", None, "First partition"),
    ("2023-01-20", "client1", None, "First partition"),
    ("2023-01-21", "client1", None, "First partition"),
]


def _write_all_passes(n_passes: int) -> List[List[int]]:
    """Write the demo sequence ``n_passes`` times; return per-pass deleted lists."""
    SuperTable(SUPER, ORG)  # bootstrap super table + default superadmin role/user
    dw = DataWriter(super_name=SUPER, organization=ORG)
    deleted_per_pass: List[List[int]] = []
    for _ in range(n_passes):
        pass_deleted: List[int] = []
        for ds in WRITE_ORDER:
            _, _, _ins, dele = dw.write(
                role_name=ROLE,
                simple_name=SIMPLE,
                data=get_dummy_data(ds),
                overwrite_columns=OVERWRITE,
            )
            pass_deleted.append(dele)
        deleted_per_pass.append(pass_deleted)
    return deleted_per_pass


def _read_live(st: SimpleTable) -> pl.DataFrame:
    query = f"SELECT day, client, value, datastream_name FROM {SIMPLE}"
    dr = DataReader(super_name=SUPER, organization=ORG, query=query)
    df, status, _msg = dr.execute(role_name=ROLE, with_scan=False, engine=engine.AUTO)
    assert str(status).endswith("OK"), f"read failed: {status}"
    return df if isinstance(df, pl.DataFrame) else pl.from_pandas(df)


def _multiset(rows) -> List[Tuple]:
    return sorted(rows, key=lambda r: (r[0], r[1], (r[2] is None, r[2]), r[3]))


@pytest.mark.parametrize("n_passes", [1, 2, 3])
def test_demo_tombstone_end_state(n_passes):
    deleted_per_pass = _write_all_passes(n_passes)

    # --- per-write deleted counts -----------------------------------------
    assert deleted_per_pass[0] == EXPECTED_DELETED_FIRST_PASS
    for later in deleted_per_pass[1:]:
        assert later == EXPECTED_DELETED_LATER_PASS

    st = SimpleTable(SuperTable(SUPER, ORG), SIMPLE)
    snap, _path = st.get_simple_table_snapshot()
    resources = snap.get("resources") or []

    # --- snapshot ----------------------------------------------------------
    assert len(resources) == EXPECTED_FILES
    assert sum(int(r.get("rows") or 0) for r in resources) == EXPECTED_SUM_ROWS
    for r in resources:
        assert st.storage.exists(r["file"]), f"missing data file {r['file']}"

    # --- stats -------------------------------------------------------------
    assert int(snap.get("stats_rows") or 0) == EXPECTED_STATS_ROWS
    stats_path = snap.get("stats_file")
    assert stats_path and st.storage.exists(stats_path)
    stats_df = _as_polars(st.storage.read_parquet(stats_path))
    assert stats_df.height == EXPECTED_STATS_ROWS

    # --- tombstone file ----------------------------------------------------
    assert int(snap.get("tombstone_rows") or 0) == EXPECTED_TOMBSTONE_ROWS
    tomb_path = snap.get("tombstone")
    assert tomb_path and st.storage.exists(tomb_path)
    dv = _as_polars(st.storage.read_parquet(tomb_path))
    assert dv.height == EXPECTED_TOMBSTONE_ROWS
    # rowids are table-unique and never reused, so the vector holds no dups.
    assert dv.get_column("__rowid__").n_unique() == EXPECTED_TOMBSTONE_ROWS
    # dead rows live in exactly four files, in a fixed 2/2/2/3 distribution.
    dead_counts = (
        dv.group_by("__file__").len().get_column("len").sort().to_list()
    )
    assert len(dead_counts) == EXPECTED_DEAD_FILE_COUNT
    assert dead_counts == EXPECTED_DEAD_COUNTS_SORTED
    # every reclaimed/dead file referenced by the vector is still a live file.
    live_files = {r["file"] for r in resources}
    assert set(dv.get_column("__file__").to_list()).issubset(live_files)

    # --- data (engine read) ------------------------------------------------
    df = _read_live(st)
    assert df.height == len(EXPECTED_LIVE_ROWS)
    # invariant: live = physical rows - tombstoned rows.
    assert df.height == EXPECTED_SUM_ROWS - EXPECTED_TOMBSTONE_ROWS
    got = _multiset(df.select(["day", "client", "value", "datastream_name"]).iter_rows())
    assert got == _multiset(EXPECTED_LIVE_ROWS)


def _as_polars(obj) -> pl.DataFrame:
    return obj if isinstance(obj, pl.DataFrame) else pl.from_arrow(obj)
