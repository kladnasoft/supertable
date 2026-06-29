"""Fault-injection regression guard for the deletion-vector carry-forward fix (audit Finding #1).

Mechanism under test
--------------------
A delete/overwrite write carries the previous deletion-vector (tombstone) forward by
reading it and unioning the new ``(file, __rowid__)`` pairs:

    data_writer.write()       reads the prev DV once       (data_writer.py:514-517)
      -> build_tombstone_file re-reads if not handed in    (processing.py:1382-1383)
                              carry-forward OR new-only     (processing.py:1384-1390)

Both reads go through ``_read_parquet_safe`` (processing.py:224-254).  Historically it
returned ``None`` for an absent object **and** for a failed read -- an object that
exists but is unreadable raises (S3Error / RuntimeError; see minio_storage.py:334-346)
and was swallowed to ``None``.  The carry-forward branch treated that ``None`` as
"there was no previous vector", so when the prev DV read FAILED and this write also
contributed new deletes, only the new pairs were written and every previously
tombstoned ``__rowid__`` silently disappeared from the vector.  The read-side anti-join
keys on ``__rowid__`` only (engine_common.py:1199), so those rows were no longer
subtracted and REAPPEARED in query results.

The fix (audit Finding #1) adds a ``required=True`` path to ``_read_parquet_safe``:
the two deletion-vector carry-forward reads (data_writer.py:~514 and the
``build_tombstone_file`` re-read in processing.py:~1383) now re-raise on a failed read
of an object whose existence cannot be ruled out, so a corrupt/transient DV read aborts
the write (leaving the prior snapshot + vector intact) instead of truncating it.

What this test does
-------------------
Seeds four rows, deletes one (building the vector), then deletes a second row while a
read failure is injected *scoped to the tombstone object only* (data/stats reads
succeed, so the second write still produces new delete pairs).  It then asserts --
through the real engine read -- the invariant that a row deleted by the FIRST write
stays deleted.

This is a regression guard for the fix: with ``required=True`` in place the second
write either aborts (the injected read failure propagates) or rewrites the full
vector, so the carry-forward is never silently truncated and the invariant holds.
Before the fix the row was resurrected and this assertion failed -- run ``git stash``
on the production change to watch it go red.
"""

from __future__ import annotations

import os

import polars as pl
import pyarrow as pa

import supertable.processing as st_processing
from supertable.data_reader import DataReader, engine
from supertable.data_writer import DataWriter
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "demo_rdv"
SIMPLE = "rfacts"
ROLE = "superadmin"
KEY = "grp"


def _rows(grps, amounts) -> pa.Table:
    return pa.table({KEY: list(grps), "amount": list(amounts)})


def _live_grps() -> list:
    dr = DataReader(super_name=SUPER, organization=ORG, query=f"SELECT {KEY} FROM {SIMPLE}")
    df, status, msg = dr.execute(role_name=ROLE, with_scan=False, engine=engine.AUTO)
    assert str(status).endswith("OK"), f"read failed: status={status} msg={msg}"
    pdf = df if isinstance(df, pl.DataFrame) else pl.from_pandas(df)
    return sorted(pdf.get_column(KEY).to_list())


def _make_tombstone_read_fail(delegate):
    """Wrap ``storage.read_parquet`` so reads of the ``/tombstone/`` object raise --
    exactly as a corrupt/transient object-store read would (minio_storage.py:345) --
    while every other read (data, stats) is delegated through unchanged."""
    def failing(path, columns=None):
        if f"{os.sep}tombstone{os.sep}" in path or "/tombstone/" in path:
            raise RuntimeError(f"injected deletion-vector read failure: {path}")
        return delegate(path, columns=columns)
    return failing


def test_dv_read_failure_resurrects_previously_deleted_row():
    SuperTable(SUPER, ORG)  # bootstrap super table + default superadmin role/user
    dw = DataWriter(super_name=SUPER, organization=ORG)

    # --- seed: four distinct keys in one data file -------------------------
    *_, deleted = dw.write(
        role_name=ROLE, simple_name=SIMPLE,
        data=_rows(["a", "b", "c", "d"], [1, 2, 3, 4]),
        overwrite_columns=[KEY],
    )
    assert deleted == 0
    assert _live_grps() == ["a", "b", "c", "d"]

    # --- delete #1: remove "a" -> builds the deletion-vector ---------------
    *_, d1 = dw.write(
        role_name=ROLE, simple_name=SIMPLE,
        data=_rows(["a"], [0]),
        overwrite_columns=[KEY], delete_only=True,
    )
    assert d1 == 1
    assert _live_grps() == ["b", "c", "d"]  # precondition: "a" is deleted

    # --- delete #2: remove "b" while the tombstone read fails --------------
    storage = st_processing._get_storage()
    delegate = storage.read_parquet
    sentinel = object()
    saved = storage.__dict__.get("read_parquet", sentinel)
    storage.read_parquet = _make_tombstone_read_fail(delegate)
    try:
        # A correctness-preserving fix may ABORT this write; tolerate that.
        try:
            dw.write(
                role_name=ROLE, simple_name=SIMPLE,
                data=_rows(["b"], [0]),
                overwrite_columns=[KEY], delete_only=True,
            )
        except Exception:
            pass
    finally:
        if saved is sentinel:
            del storage.read_parquet
        else:
            storage.read_parquet = saved

    # --- invariant: a row deleted earlier must never come back -------------
    live = _live_grps()
    assert "a" not in live, (
        f"RESURRECTION: key 'a' (deleted by delete #1) reappeared after delete #2's "
        f"deletion-vector read failed and truncated the carry-forward. live={live}"
    )
