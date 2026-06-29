"""Fault-injection regression guard for compaction's deletion-vector read (audit Finding #2).

Mechanism under test
--------------------
``DataWriter.compact()`` reads the deletion-vector once to decide whether to physically
drain tombstoned rows from the data files (data_writer.py:1186-1198):

    tombstone_path = last_simple_table.get("tombstone")
    tombstone_df   = _read_parquet_safe(tombstone_path, required=True) if tombstone_path else None
    tombstone_rows = tombstone_df.height if tombstone_df is not None else 0
    should_run_tombstones = tombstone_rows > 0

Historically that read used ``_read_parquet_safe`` (processing.py:229) on its LENIENT default
path, which returns ``None`` for both an absent vector AND a failed read.  When the read
FAILED, ``should_run_tombstones`` went False, so:

  * Phase A (``compact_tombstones``) was skipped -- no rows physically removed; and
  * the deletion-vector pointer was left intact, because the clear at data_writer.py:1288-90
    (``last_simple_table["tombstone"] = None``) is guarded by that same
    ``should_run_tombstones`` flag.

Phase B (``compact_resources``) then merged the small data files with a row-preserving
``concat_with_union`` (processing.py:467-468) keeping every original ``__rowid__``.  The
read-side anti-join is rowid-only (engine_common.py:1199), so the dead rows stayed hidden --
but they were now carried into the new compacted file behind a vector that still references a
sunset ``__file__``, making them permanently UNRECLAIMABLE.  A space/reclamation leak, never a
resurrection (the vector survived).

The fix (audit Finding #2)
--------------------------
The DV read now passes ``required=True`` (data_writer.py:~1188), the same path the write-path
carry-forward read takes (Finding #1).  A DV that exists but cannot be read no longer degrades
to "empty"; it re-raises and ABORTS the compaction, leaving the prior snapshot + vector fully
intact for a retry, so no half-merged leaky file is produced.  (An ABSENT vector still returns
``None`` -> nothing to drain -> compaction proceeds normally.)

What this test does
-------------------
Seeds two data files + a delete (building the vector), then compacts while a read failure is
injected *scoped to the tombstone object only*.  With the fix the compaction aborts, so the
test asserts the snapshot is untouched: the vector still holds the deleted rowid, the pointer
is intact, and NO merge happened (``files_after == files_before``).  Revert the ``required=True``
and the compaction instead completes the leaky Phase-B merge (``files_after < files_before``)
and this guard flips red.

Why assert at the metadata level (not via a full engine read)
-------------------------------------------------------------
Reading compacted, date-partitioned data back through the engine currently trips an
unrelated parquet-merge error (Hive partition column ``year`` resolves to ``int32`` in the
file footer vs ``dictionary`` from the path) -- a separate compaction/partition concern that
has nothing to do with the deletion vector.  So, like ``tests/characterization/test_tombstone_e2e.py``
(which reconciles the vector directly from the snapshot), this test inspects the snapshot +
deletion-vector parquet.  ``test_compact_control_drains_vector`` is the contrast: a *successful*
compaction (no fault) DRAINS the vector (Phase A removes the row and clears the pointer) and
merges the files, proving the intact-vector/no-merge result in the injected case is caused by
the abort -- never a resurrection.

Expected: both tests PASS -- the injected case aborts (vector intact, no merge); the control
drains and merges.
"""

from __future__ import annotations

import os

import polars as pl
import pyarrow as pa

import supertable.processing as st_processing
from supertable.data_writer import DataWriter
from supertable.simple_table import SimpleTable
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "demo_cdv"
SIMPLE = "cfacts"
ROLE = "superadmin"
KEY = "grp"


def _rows(grps, amounts) -> pa.Table:
    return pa.table({KEY: list(grps), "amount": list(amounts)})


def _snapshot():
    st = SimpleTable(SuperTable(SUPER, ORG), SIMPLE)
    snap, _path = st.get_simple_table_snapshot()
    return st, snap


def _dv_rowids(st, snap) -> set:
    """The set of ``__rowid__`` currently held by the snapshot's deletion-vector
    (empty if the pointer is cleared).  The tombstone dir is not date-partitioned, so
    this direct read is unaffected by the compacted-data partition-merge quirk."""
    tomb_path = snap.get("tombstone")
    if not tomb_path:
        return set()
    dv = st.storage.read_parquet(tomb_path)
    dv = dv if isinstance(dv, pl.DataFrame) else pl.from_arrow(dv)
    return set(dv.get_column("__rowid__").to_list())


def _make_tombstone_read_fail(delegate):
    """Wrap ``storage.read_parquet`` so reads of the ``/tombstone/`` object raise --
    exactly as a corrupt/transient object-store read would (minio_storage.py:345) --
    while every other read (data, stats) is delegated through unchanged."""
    def failing(path, columns=None):
        if f"{os.sep}tombstone{os.sep}" in path or "/tombstone/" in path:
            raise RuntimeError(f"injected deletion-vector read failure: {path}")
        return delegate(path, columns=columns)
    return failing


def _seed_and_delete_a(dw) -> int:
    """Two data files (a,b) + (c,d), then delete "a" to build the vector. Returns the
    rowid that the vector now tombstones."""
    dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["a", "b"], [1, 2]),
             overwrite_columns=[KEY])
    dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["c", "d"], [3, 4]),
             overwrite_columns=[KEY])
    *_, d1 = dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["a"], [0]),
                      overwrite_columns=[KEY], delete_only=True)
    assert d1 == 1
    st, snap = _snapshot()
    dv = _dv_rowids(st, snap)
    assert len(dv) == 1, f"expected exactly the rowid of 'a' in the vector, got {dv}"
    return next(iter(dv))


def test_compact_dv_read_failure_preserves_deletion_vector():
    SuperTable(SUPER, ORG)  # bootstrap super table + default superadmin role/user
    dw = DataWriter(super_name=SUPER, organization=ORG)

    deleted_rowid = _seed_and_delete_a(dw)
    files_before = len((_snapshot()[1].get("resources")) or [])

    # --- compact while the tombstone read fails ----------------------------
    storage = st_processing._get_storage()
    delegate = storage.read_parquet
    sentinel = object()
    saved = storage.__dict__.get("read_parquet", sentinel)
    storage.read_parquet = _make_tombstone_read_fail(delegate)
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

    # Guard: injection fully removed, so the inspection below reads the real vector.
    assert "read_parquet" not in st_processing._get_storage().__dict__, (
        "injection leaked past the finally block"
    )

    # --- invariant: a failed DV read aborts compaction, leaving all intact -
    st2, snap2 = _snapshot()
    after = _dv_rowids(st2, snap2)
    assert deleted_rowid in after, (
        f"RESURRECTION: compact()'s failed deletion-vector read dropped rowid "
        f"{deleted_rowid} from the vector, so the row deleted before compaction is no "
        f"longer subtracted on read. vector now holds {after}"
    )
    assert snap2.get("tombstone") is not None, (
        "the deletion-vector pointer was cleared despite the read failing -- the dead "
        "rows would resurrect on read"
    )
    # With required=True the failed DV read ABORTS the compaction, so no merge
    # happens and the snapshot is left exactly as it was. (Pre-fix, Phase B would
    # have merged here -- files_after < files_before -- carrying the dead rows into
    # a new file behind the still-live vector: the space leak. Revert required=True
    # at data_writer.py:1188 and this flips red.)
    files_after = len((snap2.get("resources")) or [])
    assert files_after == files_before, (
        f"a failed DV read must ABORT compaction (no merge), leaving the prior snapshot "
        f"intact; files_before={files_before} files_after={files_after}"
    )


def test_compact_control_drains_vector():
    """Control: an identical seed/delete then a NORMAL compaction (no fault). A healthy
    Phase A reads the vector, physically removes "a", and clears the pointer -- so the
    vector is DRAINED.  Contrast with the injected case (vector preserved): the failed
    read only ever turns a drain into a no-op-on-the-vector, never a resurrection."""
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)

    _seed_and_delete_a(dw)
    dw.compact(role_name=ROLE, simple_name=SIMPLE)

    st2, snap2 = _snapshot()
    assert snap2.get("tombstone") in (None, ""), (
        f"a successful compaction must drain (clear) the deletion-vector; "
        f"tombstone pointer is still {snap2.get('tombstone')!r}"
    )
    assert _dv_rowids(st2, snap2) == set()
    assert int(snap2.get("tombstone_rows") or 0) == 0
