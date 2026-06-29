"""Audit Finding #4 -- compaction baked Hive ``year/month/day`` into compacted
file bodies (a LocalStorage-only schema-drift / column-leak defect).

The contract
------------
The writer partitions timestamped rows into ``data_dir/year=YYYY/month=MM/
day=DD/`` and derives those keys from ``__timestamp__`` only.  Its own docstring
pins the invariant (processing.py:587-590):

    The partition columns are derived from the path only -- they are NOT
    stored as extra columns inside the Parquet file.

Fresh writes honour it: ``write_parquet_and_collect_resources`` adds temporary
``__p_year__/__p_month__/__p_day__`` keys, partitions on them, then DROPS them
before writing (processing.py:625), so a freshly written body carries only
``grp, amount, __rowid__, __timestamp__`` -- never bare ``year/month/day``.

How the body got polluted
-------------------------
Only ONE backend broke the invariant: ``LocalStorage.read_parquet`` read via
``pq.read_table(path)`` (local_storage.py), and pyarrow expands a single file
sitting at a ``year=YYYY/...`` path with INFERRED int32 ``year/month/day``
columns.  Every object-store backend (MinIO/S3/GCS/Azure) reads from a
``BytesIO`` buffer -- no path, so no inference -- and is structurally immune.

``compact_resources`` reads each source file full-width (``columns=None``,
processing.py:510), so under LocalStorage the inferred ``year/month/day`` entered
the merged frame, and the write-back (which only drops ``__p_*__``) baked them
into the compacted body.  Two consequences, both reproduced below before the fix:

  1. CORRECTNESS -- the engine scans with ``HIVE_PARTITIONING=FALSE`` +
     ``union_by_name=TRUE`` (engine_common.py), and ``year/month/day`` are not
     ``__``-prefixed, so they are never excluded: a ``SELECT *`` over compacted
     data LEAKED phantom ``year/month/day`` columns the user never wrote.
  2. OPERATIONAL -- a body that carries ``year`` (int32) AND sits at a ``year=``
     path can no longer be read by ``pq.read_table``: ``ArrowTypeError: Unable to
     merge: Field year has incompatible types: int32 vs dictionary``.  Compaction
     then can never re-read that file.

The fix (audit Finding #4)
--------------------------
Root cause, read side only: ``LocalStorage.read_parquet`` now passes
``partitioning=None`` so it returns only the file's own footer columns --
byte-for-byte consistent with the object stores; ``year/month/day`` never enter
any in-memory frame, so compaction stops baking them in (and a body that already
carries a user ``year``/``day`` column at a ``year=``/``day=`` path now reads back
without the int32-vs-dictionary conflict).

A name-based write-side strip was considered to self-heal pre-fix baked files but
REJECTED: ``year``/``month``/``day`` are valid USER column names (this repo's own
dummy data carries a ``day`` column; integer ``year`` dimensions are common), and
once compaction has merged several source files the provenance needed to tell an
inferred artifact from real data is gone -- so any blanket strip silently drops
user data.  Fix A removes the only source of baking, which is sufficient.

Expected: PASS.  Revert Fix A and the seals below flip red -- the body-schema
assertion catches the baked columns and the engine-read assertion catches the
leak.
"""

from __future__ import annotations

import collections

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

import supertable.processing as st_processing
from supertable.data_reader import DataReader
from supertable.data_writer import DataWriter
from supertable.engine.engine_enum import Engine as engine
from supertable.simple_table import SimpleTable
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "demo_f4leak"
SIMPLE = "f4facts"
ROLE = "superadmin"
KEY = "grp"

_PARTITION_COLS = ("year", "month", "day")


def _rows(grps, amounts) -> pa.Table:
    return pa.table({KEY: list(grps), "amount": list(amounts)})


def _snapshot():
    st = SimpleTable(SuperTable(SUPER, ORG), SIMPLE)
    snap, _path = st.get_simple_table_snapshot()
    return st, snap


def _body_names(file_key) -> list:
    """The file's OWN footer column names (no Hive inference). CWD is the
    hermetic ``SUPERTABLE_HOME`` so the relative resource key opens directly."""
    return list(pq.ParquetFile(file_key).schema_arrow.names)


def _baked_partition_cols(snap) -> list:
    out = []
    for r in (snap.get("resources") or []):
        leak = [c for c in _PARTITION_COLS if c in _body_names(r["file"])]
        if leak:
            out.append((r["file"], leak))
    return out


def test_compaction_does_not_bake_or_leak_partition_columns():
    SuperTable(SUPER, ORG)  # bootstrap super table + default superadmin role/user
    dw = DataWriter(super_name=SUPER, organization=ORG)

    # --- seed several small, timestamped files (each lands in today's Hive
    #     partition year=YYYY/month=MM/day=DD/) so compaction has files to merge.
    keys = ["a", "b", "c", "d", "e"]
    for i, g in enumerate(keys):
        dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows([g], [i]),
                 overwrite_columns=[KEY])

    st, snap = _snapshot()
    # Ground truth: fresh bodies honour the path-only contract (no year/month/day).
    assert _baked_partition_cols(snap) == [], (
        "fresh writes must not store partition columns in the body"
    )

    # --- normal compaction (Phase A drain + Phase B merge) ----------------
    dw.compact(role_name=ROLE, simple_name=SIMPLE)
    st2, snap2 = _snapshot()

    # PRIMARY seal #1 (no baking): no compacted body carries year/month/day.
    baked = _baked_partition_cols(snap2)
    assert baked == [], (
        f"compaction BAKED partition columns into the body (path-only contract "
        f"violated): {baked}"
    )

    # PRIMARY seal #2 (re-readable): every compacted file still reads cleanly
    # through the storage layer -- a baked int32 ``year`` beside a ``year=`` path
    # would raise the int32-vs-dictionary merge error here.
    for r in snap2.get("resources") or []:
        df = st_processing._read_parquet_safe(r["file"], required=True)
        assert df is not None, f"compacted file vanished: {r['file']}"
        assert not (set(_PARTITION_COLS) & set(df.columns)), (
            f"compacted body leaked partition cols on read: {df.columns}"
        )

    # PRIMARY seal #3 (no leak): a SELECT * through the real engine returns the
    # user's columns only -- never the phantom year/month/day.
    dr = DataReader(super_name=SUPER, organization=ORG, query=f"SELECT * FROM {SIMPLE}")
    out, status, msg = dr.execute(role_name=ROLE, with_scan=False, engine=engine.AUTO)
    assert str(status).endswith("OK"), f"engine read failed: {status} / {msg}"
    out = out if isinstance(out, pl.DataFrame) else pl.from_pandas(out)
    leaked = [c for c in _PARTITION_COLS if c in out.columns]
    assert leaked == [], (
        f"engine read LEAKED partition columns the user never wrote: {leaked} "
        f"(full columns: {out.columns})"
    )
    # Data intact: every seeded key survives exactly once, value column present.
    assert "amount" in out.columns
    assert collections.Counter(out.get_column(KEY).to_list()) == \
        collections.Counter(keys), f"row set changed after compaction: {out}"

    # The compacted file is re-compactable (no stuck/un-readable file).
    dw.compact(role_name=ROLE, simple_name=SIMPLE)


def test_user_day_column_survives_compaction():
    """A LEGITIMATE user column named ``day`` (the exact name the Hive layout
    also uses for its path segment) must survive a write+compact round-trip.

    This is the guard that rejected a name-based write-side strip: such a strip
    would silently delete this column.  Fix A keeps it because it never injects
    a partition column to begin with, so the body's real ``day`` is preserved and
    read back through the path-only contract."""
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)
    sname = "f4userday"

    # Two small files, each with a real string ``day`` column, same overwrite key.
    dw.write(role_name=ROLE, simple_name=sname,
             data=pa.table({KEY: ["x"], "day": ["Monday"], "amount": [1]}),
             overwrite_columns=[KEY])
    dw.write(role_name=ROLE, simple_name=sname,
             data=pa.table({KEY: ["y"], "day": ["Tuesday"], "amount": [2]}),
             overwrite_columns=[KEY])

    dw.compact(role_name=ROLE, simple_name=sname)

    dr = DataReader(super_name=SUPER, organization=ORG,
                    query=f"SELECT {KEY}, day, amount FROM {sname}")
    out, status, msg = dr.execute(role_name=ROLE, with_scan=False, engine=engine.AUTO)
    assert str(status).endswith("OK"), f"engine read failed: {status} / {msg}"
    out = out if isinstance(out, pl.DataFrame) else pl.from_pandas(out)
    got = {k: d for k, d in out.select([KEY, "day"]).iter_rows()}
    assert got == {"x": "Monday", "y": "Tuesday"}, (
        f"user 'day' column was corrupted/dropped by compaction: {got}"
    )
