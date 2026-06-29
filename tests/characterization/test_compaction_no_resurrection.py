"""§5 no-resurrection seal: compaction must physically drain tombstoned rows.

The §5 question
---------------
``compact_resources(small_only=True)`` (processing.py:414) is **row-preserving** --
it merges files with ``concat_with_union`` and does NOT drop any row (its own
docstring, processing.py:462-471).  Tombstone-driven removal is a SEPARATE
pre-step, ``compact_tombstones`` ("Phase A"), run by ``DataWriter.compact()``
*before* the merge.  So the no-resurrection guarantee rests entirely on Phase A
running first: if the merge ever copied a tombstoned row into the new file
without Phase A having physically removed it, the row would be carried forward
behind a now-cleared pointer and **resurrect on read**.

(This is the same ordering Finding #2 protected: a swallowed deletion-vector
read set ``should_run_tombstones=False``, skipping Phase A *and* the pointer
clear, so Phase B merged the dead rows forward -- a space leak there because the
vector survived; an outright resurrection here if the pointer were also cleared.)

What this test does
-------------------
Seeds four keys across separate files, deletes one ("a") -- so the vector holds
exactly a's ``__rowid__`` while a's row is still PHYSICALLY in its file -- then
runs a NORMAL ``compact()`` and proves a is gone *for good*:

  1. Pre-compact: reconcile live rows the way the read path does -- union every
     resource file's ``(key, __rowid__)`` then subtract the deletion-vector by
     ``__rowid__`` (the read-side anti-join is rowid-only, engine_common.py:1199).
     Live == {b,c,d}; a is hidden by the vector but still physically present.
  2. compact().
  3. Post-compact PHYSICAL check: a must be ABSENT from every resource file --
     Phase A drained it.  This is the assertion that actually rules out
     resurrection: if a were still physical *and* the pointer cleared, it would
     reappear on read.
  4. Post-compact reconcile: every survivor b,c,d is still present (nothing
     lost) and 'a' is absent (nothing resurrected), and the vector is drained
     consistently with a's physical removal.  (Membership, not exact count: a
     SEPARATE compaction bookkeeping defect can list a Phase-A output twice in
     the snapshot -- pinned by test_compaction_duplicate_resource_entry.py --
     which is orthogonal to resurrection and must not mask this seal.)

Why reconcile at the file+vector level instead of a full engine read
--------------------------------------------------------------------
Reading compacted, (timestamp-)partitioned data back through the engine trips an
unrelated parquet-merge quirk (Hive ``year`` int32-in-footer vs dictionary-from-
path) -- see ``test_tombstone_compact_read_failure.py``.  So, like that test, we
read the individual resource files + the deletion-vector parquet directly (single
-file reads sidestep the multi-file partition merge) and apply the read path's
own rowid anti-join by hand.  ``test_compact_control_drains_vector`` is the
adjacent proof that compact() drains the pointer; this test adds the missing
half -- that the drained row is also physically gone, so the cleared pointer
cannot resurrect it.

Expected: PASS -- compaction is order-correct, so §5 (resurrection-on-compaction)
is NOT a real bug.  Were Phase A ever skipped/misordered, step 3 would find a
physically present, no-longer-tombstoned 'a' and this seal would flip red.
"""

from __future__ import annotations

import collections

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from supertable.data_writer import DataWriter
from supertable.simple_table import SimpleTable
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "demo_nores"
SIMPLE = "nrfacts"
ROLE = "superadmin"
KEY = "grp"


def _rows(grps, amounts) -> pa.Table:
    return pa.table({KEY: list(grps), "amount": list(amounts)})


def _snapshot():
    st = SimpleTable(SuperTable(SUPER, ORG), SIMPLE)
    snap, _path = st.get_simple_table_snapshot()
    return st, snap


def _dv_rowids(st, snap) -> set:
    """The ``__rowid__`` set the snapshot's deletion-vector currently tombstones
    (empty if the pointer is cleared)."""
    tomb_path = snap.get("tombstone")
    if not tomb_path:
        return set()
    dv = st.storage.read_parquet(tomb_path)
    dv = dv if isinstance(dv, pl.DataFrame) else pl.from_arrow(dv)
    return set(dv.get_column("__rowid__").to_list())


def _read_data_file(file_key) -> pl.DataFrame:
    """Read ONE data file's own footer columns with hive partition inference OFF.

    Compaction bakes the partition column ``year`` into the file body (int32),
    while the hive path ``year=YYYY/...`` implies ``year`` as a dictionary, so the
    default partition-aware read (``storage.read_parquet`` -> ``pq.read_table``)
    trips an int32-vs-dictionary merge on POST-compaction files -- an unrelated
    partition quirk also documented in ``test_tombstone_compact_read_failure.py``.
    ``pq.ParquetFile(...).read()`` opens just this one file (no directory/partition
    discovery), so it returns the footer columns (incl. ``grp``/``__rowid__``)
    unaffected by that quirk."""
    return pl.from_arrow(pq.ParquetFile(file_key).read())


def _physical_key_counts(st, snap) -> collections.Counter:
    """Multiset of every key PHYSICALLY present across all resource files,
    ignoring the vector -- i.e. what would surface on read if nothing were
    subtracted."""
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


def test_compaction_does_not_resurrect_deleted_rows():
    SuperTable(SUPER, ORG)  # bootstrap super table + default superadmin role/user
    dw = DataWriter(super_name=SUPER, organization=ORG)

    # --- seed: 'a' SHARES a file with 'b'; 'c' and 'd' get their own ------
    # 'a' must co-reside with a survivor: a sole-row file is reclaimed outright
    # on delete (the e2e suite's H1 -- a fully-tombstoned file is dropped from
    # the snapshot immediately), removing 'a' before compaction ever runs.  With
    # 'b' alongside, deleting 'a' leaves the file partially live, so 'a' stays
    # physically present + tombstoned -- the exact state Phase A must surgically
    # drain while keeping 'b'.
    dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["a", "b"], [1, 2]),
             overwrite_columns=[KEY])
    dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["c"], [3]),
             overwrite_columns=[KEY])
    dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["d"], [4]),
             overwrite_columns=[KEY])

    # --- delete "a" -> vector holds a's rowid; a still PHYSICALLY present --
    *_, deleted = dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["a"], [0]),
                           overwrite_columns=[KEY], delete_only=True)
    assert deleted == 1

    st, snap = _snapshot()
    # Pre-compact ground truth: a is hidden by the vector (live == b,c,d) but the
    # row itself is still on disk -- exactly the state compaction must resolve.
    assert _live_key_counts(st, snap) == collections.Counter({"b": 1, "c": 1, "d": 1}), (
        "pre-compact live set should already exclude the deleted 'a' via the vector"
    )
    assert _physical_key_counts(st, snap)["a"] == 1, (
        "pre-compact 'a' should still be physically present (only logically deleted) "
        "-- otherwise the post-compact drain assertion would be vacuous"
    )
    assert len(_dv_rowids(st, snap)) == 1, "vector should hold exactly a's rowid pre-compact"

    # --- NORMAL compaction: Phase A drains, Phase B merges survivors ------
    dw.compact(role_name=ROLE, simple_name=SIMPLE)

    st2, snap2 = _snapshot()

    # PRIMARY no-resurrection seal: 'a' is now PHYSICALLY ABSENT from every file.
    # Phase A removed it before the row-preserving merge could carry it forward,
    # so even though the vector pointer is now cleared there is nothing left to
    # resurrect.  A skipped/misordered Phase A would leave 'a' on disk here.
    phys_after = _physical_key_counts(st2, snap2)
    assert phys_after["a"] == 0, (
        f"RESURRECTION: 'a' is still physically present after compaction "
        f"({phys_after['a']} row(s)); Phase A failed to drain it before the "
        f"row-preserving merge, so it will reappear on read. physical={dict(phys_after)}"
    )

    live_after = _live_key_counts(st2, snap2)
    # The deleted key stays gone on the read-reconciled set too (belt and braces
    # with the physical check above): 'a' must not reappear among live rows.
    assert "a" not in live_after, (
        f"RESURRECTION: 'a' reappeared in the reconciled live set after compaction: "
        f"{dict(live_after)}"
    )
    # No survivor was lost.  Membership, not multiplicity: a SEPARATE compaction
    # defect (a Phase-A output not consumed by Phase B is listed twice in the
    # snapshot -- see test_compaction_duplicate_resource_entry.py) can inflate a
    # survivor's count, which is orthogonal to resurrection and must not mask
    # this seal.  What matters for §5 is that every survivor is still present and
    # the deleted row is not.
    assert {"b", "c", "d"} <= set(live_after), (
        f"compaction LOST a survivor; expected b,c,d all present, got {dict(live_after)}"
    )

    # The vector is drained consistently with a's physical removal (Phase A both
    # removed the row and cleared the pointer): no dangling tombstone, no leak.
    assert _dv_rowids(st2, snap2) == set(), (
        "successful compaction must drain the vector once the row is physically gone"
    )
    assert int(snap2.get("tombstone_rows") or 0) == 0
