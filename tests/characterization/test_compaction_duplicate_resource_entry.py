"""§5 regression guard: compaction must list each data file at most ONCE.

The defect this guards against (now fixed)
------------------------------------------
``DataWriter.compact()`` runs two phases and originally handed BOTH phases' new
files to ``simple_table.update`` as ``new_resources`` -- but a Phase-A output had
ALREADY been spliced into the in-memory snapshot's ``resources`` before Phase B
ran:

  * Phase A (``compact_tombstones``) rewrites each tombstoned file; the caller
    splices its outputs into ``last_simple_table["resources"]`` immediately
    (data_writer.py:1223-1229).
  * The aggregate step built ``all_new_resources = tomb_new + small_new`` and the
    update call passed that whole list as ``new_resources``.
  * ``simple_table.update`` does ``(baseline - sunset) + new_resources`` with NO
    dedup (simple_table.py:353-354).  So any Phase-A output that Phase B did NOT
    consume sat in BOTH the baseline (spliced at 1228, not sunset) AND
    ``new_resources`` -> the **same file listed twice** in the new snapshot.

A Phase-A output goes "un-consumed by Phase B" two ways -- one backend-independent
(``small_only`` skips a large rewritten file) and one I/O-driven (its Phase-B read
fails: ``compact_resources`` reads leniently and "leaves the file in the snapshot").

Impact was Medium: a duplicated resource entry double-counts physical rows/stats,
wastes a full file read per query, and violates the "each file referenced once"
invariant.  User-visible SELECTs were likely masked by the read view's merge-key
dedup (``ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY __timestamp__ DESC)``),
but the metadata corruption was real and could compound across compactions.

The fix
-------
``compact()`` now hands ``simple_table.update`` only Phase B's brand-new files
(``update_new_resources``), since Phase A's outputs are already in the baseline it
merges against; ``all_new_resources`` is still used (unchanged) for stats
extraction, the schema model_df and the result metrics (data_writer.py, the
"Aggregate the two phases" block).

What this test does
-------------------
Reproduces the exact previously-duplicating scenario: 'a' co-resides with 'b' (so
deleting 'a' makes Phase A *rewrite* the file rather than reclaim it outright),
while 'c'/'d' are separate small files Phase B can merge -- isolating a Phase-A
output ({b}) that Phase B does not consume (here because re-reading the freshly
written, Hive-pathed file trips the pyarrow ``year`` int32-vs-dictionary quirk,
the same one in test_tombstone_compact_read_failure.py).  It then asserts the new
snapshot references every file exactly once.

Expected: PASS.  Revert the ``update_new_resources`` change (pass
``all_new_resources`` back to ``simple_table.update``) and the un-consumed {b}
file is listed twice -> ``entries == distinct`` flips red.
"""

from __future__ import annotations

import pyarrow as pa

from supertable.data_writer import DataWriter
from supertable.simple_table import SimpleTable
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "demo_dupres"
SIMPLE = "drfacts"
ROLE = "superadmin"
KEY = "grp"


def _rows(grps, amounts) -> pa.Table:
    return pa.table({KEY: list(grps), "amount": list(amounts)})


def _resource_files(snap) -> list:
    return [r["file"] for r in (snap.get("resources") or [])
            if isinstance(r, dict) and r.get("file")]


def _snapshot():
    st = SimpleTable(SuperTable(SUPER, ORG), SIMPLE)
    snap, _path = st.get_simple_table_snapshot()
    return st, snap


def test_compaction_lists_each_file_at_most_once():
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)

    # 'a' co-resides with 'b' (deleting 'a' rewrites the file in Phase A rather
    # than reclaiming it outright); 'c' and 'd' are separate small files Phase B
    # CAN merge -- isolating the un-consumed Phase-A file ({b}), the exact input
    # that used to be listed twice.
    dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["a", "b"], [1, 2]),
             overwrite_columns=[KEY])
    dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["c"], [3]),
             overwrite_columns=[KEY])
    dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["d"], [4]),
             overwrite_columns=[KEY])
    *_, deleted = dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["a"], [0]),
                           overwrite_columns=[KEY], delete_only=True)
    assert deleted == 1

    dw.compact(role_name=ROLE, simple_name=SIMPLE)

    _st, snap = _snapshot()
    files = _resource_files(snap)
    dupes = sorted(f for f in set(files) if files.count(f) > 1)
    assert len(files) == len(set(files)), (
        f"compaction left a DUPLICATE resource entry {dupes}: the new snapshot must "
        f"reference each file exactly once (Phase-A outputs are already in the update "
        f"baseline and must not be re-added as new_resources). files={files}"
    )
    # The un-consumed Phase-A output ({b}) is still present -- exactly once, not lost.
    assert files, "compaction unexpectedly produced an empty resource list"
