"""Audit Finding #3 -- a swallowed DATA-FILE read inside ``compact_tombstones``
resurrects deleted rows (the uncovered sibling of Findings #1 and #2).

Where this differs from the two prior fault-injection guards
-----------------------------------------------------------
Findings #1/#2 hardened the read of the deletion-vector *pointer* with
``required=True`` (data_writer.py:518 write path; data_writer.py:1196 compact),
so a corrupt/transient read of the ``/tombstone/`` object now ABORTS instead of
degrading to "empty".  ``test_tombstone_dv_read_failure.py`` and
``test_tombstone_compact_read_failure.py`` pin those -- and both scope their
injected failure to the ``/tombstone/`` object *only*, delegating every
DATA-file read through unchanged (test_tombstone_compact_read_failure.py:108-110).

Historically one read site on the drain path was left unguarded: the
PER-DATA-FILE read INSIDE ``compact_tombstones`` itself ::

    existing_df = _read_parquet_safe(file_path, profiler=p, file_size=file_size)
    if existing_df is None or ROWID_COL not in existing_df.columns:
        continue                                   # processing.py:2268-2270

That read took ``_read_parquet_safe``'s LENIENT default (``required=False``),
so a transient backend error on an object whose existence cannot be ruled out was
swallowed to ``None`` and the file silently skipped -- its tombstoned rows were
NOT physically removed and it was NOT added to ``sunset_files``.  Phase B
(``compact_resources``) is row-preserving and only drops dead rows when handed an
explicit ``dead_rowids`` set, which these callers never pass (processing.py:519);
so Phase A is the ONLY physical drain.  Meanwhile the caller clears the pointer
UNCONDITIONALLY once the vector was non-empty -- ``last_simple_table["tombstone"]
= None`` (data_writer.py:1315-1317 compact; data_writer.py:690 write path) -- with
no check that every named file actually drained -> the dead row resurrected.

The fix (audit Finding #3) makes that per-file drain read ``required=True``
(processing.py:2268), the same path Findings #1/#2 took for the DV-pointer reads:
a data file that exists but cannot be read now RAISES out of ``compact_tombstones``
and ABORTS the write/compact, leaving the prior snapshot + vector intact for a
retry instead of clearing the pointer over an undrained file.  (A genuine ABSENCE
still returns ``None`` -> safe skip, since a sunset/raced file's rows are already
gone.)

Net effect: the dead row stays physically on disk while the vector that hid it
(read-side anti-join is rowid-only, engine_common.py:1199) is dropped -> the row
RESURRECTS on the next read.  This is exactly the failure mode the
``_read_parquet_safe`` docstring calls out (processing.py:238-245: a swallowed
carry-forward read "would resurrect deleted rows") -- only the guard was placed on
the pointer read, not on the per-file reads of the function that performs the
physical drain the pointer-clear assumes succeeded.

What this test does
-------------------
Seeds 'a' alongside survivor 'b' in one file (so deleting 'a' leaves the file
partially live -- a sole-'a' file would be reclaimed outright before compaction,
making the drain vacuous), plus 'c' and 'd' in their own files.  Deletes 'a' ->
the vector holds exactly a's ``__rowid__`` with ``__file__`` = a's data file,
while a's row is still PHYSICALLY present.  Then runs ``compact()`` with a read
failure injected *scoped to a's data file only* -- the ``/tombstone/`` read still
succeeds, so the DV-pointer guard (Finding #2) does NOT trip and ``compact()``
proceeds into Phase A, where the data-file read is the one that fails.

After compaction (injection removed) it reconciles live rows the way the read
path does -- union every resource file's ``(key, __rowid__)`` then subtract the
vector by ``__rowid__`` -- and asserts the deleted 'a' did NOT come back.

Expected: PASS -- with the drain read now ``required=True`` the faulted compaction
aborts (the test tolerates that), so 'a' stays tombstoned and is never resurrected:
it remains physically present but its rowid stays in the live vector, so the read
path keeps subtracting it.  Revert ``required=True`` at processing.py:2268 and this
guard flips red -- the assertion below catches a physical 'a' behind a cleared
pointer.
"""

from __future__ import annotations

import collections

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

import supertable.processing as st_processing
from supertable.data_writer import DataWriter
from supertable.simple_table import SimpleTable
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "demo_cdf"
SIMPLE = "cdffacts"
ROLE = "superadmin"
KEY = "grp"


def _rows(grps, amounts) -> pa.Table:
    return pa.table({KEY: list(grps), "amount": list(amounts)})


def _snapshot():
    st = SimpleTable(SuperTable(SUPER, ORG), SIMPLE)
    snap, _path = st.get_simple_table_snapshot()
    return st, snap


def _dv_frame(st, snap):
    """The snapshot's deletion-vector as a polars frame (``__file__`` +
    ``__rowid__``), or ``None`` when the pointer is cleared."""
    tomb_path = snap.get("tombstone")
    if not tomb_path:
        return None
    dv = st.storage.read_parquet(tomb_path)
    return dv if isinstance(dv, pl.DataFrame) else pl.from_arrow(dv)


def _dv_rowids(st, snap) -> set:
    dvf = _dv_frame(st, snap)
    if dvf is None:
        return set()
    return set(dvf.get_column("__rowid__").to_list())


def _read_data_file(file_key) -> pl.DataFrame:
    """Read ONE data file's own footer columns with hive partition inference OFF
    (single-file open sidesteps the post-compaction ``year`` int32-vs-dictionary
    partition-merge quirk documented in ``test_tombstone_compact_read_failure.py``)."""
    return pl.from_arrow(pq.ParquetFile(file_key).read())


def _physical_key_counts(snap) -> collections.Counter:
    c: collections.Counter = collections.Counter()
    for r in (snap.get("resources") or []):
        c.update(_read_data_file(r["file"]).get_column(KEY).to_list())
    return c


def _live_key_counts(st, snap) -> collections.Counter:
    """Reconcile live rows exactly as the read path does: union every resource
    file's ``(key, __rowid__)`` then drop rows whose ``__rowid__`` is in the
    deletion-vector (rowid-only anti-join, engine_common.py:1199)."""
    dv = _dv_rowids(st, snap)
    c: collections.Counter = collections.Counter()
    for r in (snap.get("resources") or []):
        df = _read_data_file(r["file"])
        for key, rowid in df.select([KEY, "__rowid__"]).iter_rows():
            if rowid not in dv:
                c.update([key])
    return c


def _make_datafile_read_fail(delegate, target_file):
    """Wrap ``storage.read_parquet`` so reads of exactly ``target_file`` (a's data
    file) raise -- as a corrupt/transient object-store read would -- while every
    other read (the ``/tombstone/`` object, stats, the other data files) is
    delegated through unchanged.  The tombstone read therefore SUCCEEDS, so the
    Finding #2 DV-pointer guard does not trip and ``compact()`` enters Phase A,
    where this data-file read is the one that fails."""
    def failing(path, columns=None):
        if path == target_file:
            raise RuntimeError(
                f"injected data-file read failure during Phase A drain: {path}"
            )
        return delegate(path, columns=columns)
    return failing


def test_compact_datafile_read_failure_does_not_resurrect():
    SuperTable(SUPER, ORG)  # bootstrap super table + default superadmin role/user
    dw = DataWriter(super_name=SUPER, organization=ORG)

    # --- seed: 'a' SHARES a file with survivor 'b'; 'c','d' get their own ----
    dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["a", "b"], [1, 2]),
             overwrite_columns=[KEY])
    dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["c"], [3]),
             overwrite_columns=[KEY])
    dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["d"], [4]),
             overwrite_columns=[KEY])

    # --- delete 'a' -> vector holds a's rowid + __file__; a still physical ---
    *_, deleted = dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["a"], [0]),
                           overwrite_columns=[KEY], delete_only=True)
    assert deleted == 1

    st, snap = _snapshot()
    dvf = _dv_frame(st, snap)
    assert dvf is not None and dvf.height == 1, "vector should hold exactly a's row"
    deleted_rowid = dvf.get_column("__rowid__").to_list()[0]
    target_file = dvf.get_column("__file__").to_list()[0]

    # Ground truth pre-compact: 'a' hidden by the vector, still on disk.
    assert _live_key_counts(st, snap) == collections.Counter({"b": 1, "c": 1, "d": 1})
    assert _physical_key_counts(snap)["a"] == 1
    assert target_file in {r["file"] for r in snap["resources"]}, (
        "the vector's __file__ must name a live resource we can target"
    )

    # --- compact() while ONLY a's data file fails to read ------------------
    storage = st_processing._get_storage()
    delegate = storage.read_parquet
    sentinel = object()
    saved = storage.__dict__.get("read_parquet", sentinel)
    storage.read_parquet = _make_datafile_read_fail(delegate, target_file)
    try:
        # A correctness-preserving fix may ABORT the compaction; tolerate that.
        try:
            dw.compact(role_name=ROLE, simple_name=SIMPLE)
        except Exception:
            pass
    finally:
        if saved is sentinel:
            del storage.read_parquet
        else:
            storage.read_parquet = saved
    assert "read_parquet" not in st_processing._get_storage().__dict__, (
        "injection leaked past the finally block"
    )

    # --- invariant: the row deleted before compaction stays gone ----------
    st2, snap2 = _snapshot()
    live_after = _live_key_counts(st2, snap2)
    phys_after = _physical_key_counts(snap2)
    dv_after = _dv_rowids(st2, snap2)

    # The actual resurrection: 'a' is still physically present (Phase A skipped
    # it on the failed read) AND its rowid is no longer subtracted by the vector
    # (the pointer was cleared anyway) -> it surfaces on read.
    resurrected_physically = phys_after["a"] >= 1 and deleted_rowid not in dv_after
    assert not resurrected_physically, (
        f"RESURRECTION: 'a' is physically present after compaction "
        f"(physical={dict(phys_after)}) but its rowid {deleted_rowid} is NOT in the "
        f"vector {dv_after} (pointer cleared despite the skipped drain) -- it will "
        f"reappear on read. A failed Phase-A data-file read must either abort the "
        f"compaction or keep the vector live for the undrained file."
    )
    assert "a" not in live_after, (
        f"RESURRECTION: 'a' reappeared in the reconciled live set after a faulted "
        f"compaction: {dict(live_after)}"
    )
    # No survivor lost (membership, not multiplicity -- a separate duplicate-resource
    # bookkeeping defect can inflate counts and must not mask this seal).
    assert {"b", "c", "d"} <= set(live_after), (
        f"compaction LOST a survivor; expected b,c,d present, got {dict(live_after)}"
    )
