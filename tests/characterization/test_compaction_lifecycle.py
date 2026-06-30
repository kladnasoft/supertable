"""Post-compaction lifecycle: the deletion-vector AND the stats must be drained.

Two failure modes a *correct* compaction has to avoid, both raised directly by
the user:

  1. **"Every insert triggers a compaction."**  When the tombstone threshold
     fires and Phase A physically removes the dead rows from the parquet, it
     must ALSO clear the deletion-vector content (pointer + row count).  If it
     drained the rows but left the vector pointing at its old height, every
     later write would see ``height >= max_tombstone_rows`` and re-drain a now
     empty vector — a permanent compaction-on-every-insert overload.

  2. **"We must get rid of the sunset file stats."**  Compaction sunsets the
     old data files (Phase A's drained-away originals, Phase B's merged-away
     smalls) and writes replacements.  The external column-statistics parquet
     must drop the sunset files' rows and carry only the live files; otherwise
     it grows without bound and the read-path pruner consults stats for files
     that no longer exist.

Both behaviours are already implemented (the write path clears
``tombstone``/``tombstone_rows`` after Phase A and passes the full
``sunset_files`` set to ``build_stats_file``).  These tests SEAL that contract
end-to-end against the real ``DataWriter`` — the unit suite proves
``build_stats_file`` filters ``removed_files`` in isolation
(``test_processing_stats.py``); here we prove the *write path* actually hands it
the right sunset set and that the gate genuinely stops re-firing.

Run against the per-test hermetic fake Redis (``tests/conftest.py``); a low
``max_tombstone_rows`` makes the threshold drain deterministic, and a high
``max_overlapping_files`` keeps the small-file gate shut so the only trigger in
play for the tombstone tests is the deletion-vector threshold.
"""
from __future__ import annotations

import collections
import logging
import uuid

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from supertable.data_writer import DataWriter
from supertable.simple_table import SimpleTable
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "demo"
ROLE = "superadmin"
KEY = "grp"
LOGGER = "supertable.config.defaults"  # where DataWriter's ``logger`` lives


def _rows(keys) -> pa.Table:
    return pa.table({KEY: list(keys), "amount": [0] * len(keys)})


def _compaction_lines(caplog) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if "compaction during write" in r.getMessage()
    ]


def _snapshot(simple):
    st = SimpleTable(SuperTable(SUPER, ORG), simple)
    snap, _path = st.get_simple_table_snapshot()
    return st, snap


def _live_files(snap) -> set:
    return {r["file"] for r in (snap.get("resources") or [])}


def _stats_df(st, snap) -> pl.DataFrame:
    sp = snap.get("stats_file")
    if not sp:
        return pl.DataFrame()
    df = st.storage.read_parquet(sp)
    return df if isinstance(df, pl.DataFrame) else pl.from_arrow(df)


def _stats_paths(st, snap) -> set:
    df = _stats_df(st, snap)
    if "file_path" not in df.columns:
        return set()
    return set(df.get_column("file_path").to_list())


def _physical_keys(snap) -> collections.Counter:
    """Multiset of every key PHYSICALLY present across resource files.

    Reads each resource file on its own (``pq.ParquetFile``) so the per-file
    footer columns surface without tripping the partition-merge quirk that a
    directory-level read of compacted, hive-partitioned data hits (documented in
    ``test_tombstone_compact_read_failure.py``)."""
    c: collections.Counter = collections.Counter()
    for r in (snap.get("resources") or []):
        df = pl.from_arrow(pq.ParquetFile(r["file"]).read())
        c.update(df.get_column(KEY).to_list())
    return c


def test_tombstone_threshold_drain_clears_vector_and_stats(caplog):
    """The threshold-crossing delete drains inline, clears the vector, and the
    sunset files' stats rows are removed."""
    simple = f"tomb_drain_{uuid.uuid4().hex[:8]}"
    SuperTable(SUPER, ORG)  # bootstrap super + default superadmin role
    dw = DataWriter(super_name=SUPER, organization=ORG)
    # Tombstone gate trips at 2 dead rows; keep the small-file gate shut.
    dw._table_config_cache[simple] = {
        "max_tombstone_rows": 2,
        "max_overlapping_files": 100,
    }

    # Two 2-row files.  'a'/'c' will be deleted but co-reside with survivors
    # 'b'/'d', so neither file is eagerly reclaimed — the dead rows stay in the
    # deletion-vector until the threshold drains them.
    dw.write(role_name=ROLE, simple_name=simple, data=_rows(["a", "b"]),
             overwrite_columns=[KEY])
    dw.write(role_name=ROLE, simple_name=simple, data=_rows(["c", "d"]),
             overwrite_columns=[KEY])

    with caplog.at_level(logging.INFO, logger=LOGGER):
        # Delete #1 — vector height 1 (< 2): no compaction yet.
        caplog.clear()
        *_, deleted1 = dw.write(role_name=ROLE, simple_name=simple,
                                data=_rows(["a"]), overwrite_columns=[KEY],
                                delete_only=True)
        assert deleted1 == 1
        assert _compaction_lines(caplog) == [], "1 dead row < threshold must not compact"

        st_mid, snap_mid = _snapshot(simple)
        assert snap_mid.get("tombstone"), "vector should be live (1 row) below threshold"
        assert int(snap_mid.get("tombstone_rows") or 0) == 1
        files_before_drain = _live_files(snap_mid)  # the two original files

        # Delete #2 — vector height 2 (>= 2): inline Phase A drains on THIS write.
        caplog.clear()
        *_, deleted2 = dw.write(role_name=ROLE, simple_name=simple,
                                data=_rows(["c"]), overwrite_columns=[KEY],
                                delete_only=True)
        assert deleted2 == 1
        drain_lines = _compaction_lines(caplog)

    # The crossing write logged a compaction whose trigger names the threshold.
    assert len(drain_lines) == 1, f"expected one compaction line, got {drain_lines}"
    assert "trigger=tombstone_threshold" in drain_lines[0], drain_lines[0]
    assert "tombstone phase removed 2 row(s)" in drain_lines[0], drain_lines[0]

    # --- concern #1: the vector content is gone, so the gate can't re-trip ---
    st, snap = _snapshot(simple)
    assert snap.get("tombstone") is None, "deletion-vector pointer must be cleared after drain"
    assert int(snap.get("tombstone_rows") or 0) == 0

    # Physically drained: a and c are gone for good; survivors remain.
    phys = _physical_keys(snap)
    assert phys["a"] == 0 and phys["c"] == 0, f"drained rows still on disk: {dict(phys)}"
    assert phys["b"] == 1 and phys["d"] == 1, f"survivor lost: {dict(phys)}"

    # --- concern #2: the sunset files' stats rows are dropped ---
    live_files = _live_files(snap)
    stats_paths = _stats_paths(st, snap)
    assert files_before_drain.isdisjoint(live_files), "pre-drain files must be sunset"
    assert files_before_drain.isdisjoint(stats_paths), \
        "sunset files' stats rows must be dropped, not carried forward"
    assert stats_paths == live_files, "stats must describe exactly the live files"
    assert int(snap.get("stats_rows") or 0) == _stats_df(st, snap).height

    # --- concern #1 (the crux): a later plain append does NOT re-compact ---
    with caplog.at_level(logging.INFO, logger=LOGGER):
        caplog.clear()
        dw.write(role_name=ROLE, simple_name=simple, data=_rows(["e"]),
                 overwrite_columns=[KEY])
        post_lines = _compaction_lines(caplog)
    assert post_lines == [], f"append after drain must not re-trigger compaction: {post_lines}"

    _, snap2 = _snapshot(simple)
    assert snap2.get("tombstone") is None, "append after drain must not resurrect a vector"
    phys2 = _physical_keys(snap2)
    assert phys2["a"] == 0 and phys2["c"] == 0
    assert phys2["b"] == 1 and phys2["d"] == 1 and phys2["e"] == 1


def test_appends_after_drain_do_not_recompact(caplog):
    """The literal rebuttal of "every insert triggers a compaction": once the
    vector is drained, a burst of plain appends stays quiet."""
    simple = f"no_recompact_{uuid.uuid4().hex[:8]}"
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)
    dw._table_config_cache[simple] = {
        "max_tombstone_rows": 2,
        "max_overlapping_files": 100,
    }

    # Reach a drained state: build the vector to the threshold, drain inline.
    dw.write(role_name=ROLE, simple_name=simple, data=_rows(["a", "b"]),
             overwrite_columns=[KEY])
    dw.write(role_name=ROLE, simple_name=simple, data=_rows(["c", "d"]),
             overwrite_columns=[KEY])
    dw.write(role_name=ROLE, simple_name=simple, data=_rows(["a"]),
             overwrite_columns=[KEY], delete_only=True)
    with caplog.at_level(logging.INFO, logger=LOGGER):
        caplog.clear()
        dw.write(role_name=ROLE, simple_name=simple, data=_rows(["c"]),
                 overwrite_columns=[KEY], delete_only=True)
        assert len(_compaction_lines(caplog)) == 1, "the crossing delete must drain once"

    _, snap = _snapshot(simple)
    assert snap.get("tombstone") is None and int(snap.get("tombstone_rows") or 0) == 0

    # Hammer the table with plain appends.  If the vector content had NOT been
    # cleared, each would still read height >= threshold and re-drain — the
    # overload the fix prevents.
    with caplog.at_level(logging.INFO, logger=LOGGER):
        for k in ["e", "f", "g", "h", "i"]:
            caplog.clear()
            dw.write(role_name=ROLE, simple_name=simple, data=_rows([k]),
                     overwrite_columns=[KEY])
            assert _compaction_lines(caplog) == [], \
                f"append {k!r} wrongly re-triggered compaction"
            _, s = _snapshot(simple)
            assert s.get("tombstone") is None, f"append {k!r} resurrected a vector pointer"


def test_small_file_gate_compaction_removes_sunset_file_stats(caplog):
    """Phase B (small-file merge) sunsets the originals; their stats rows must
    not survive into the new stats artifact."""
    simple = f"sunset_stats_{uuid.uuid4().hex[:8]}"
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)
    dw._table_config_cache[simple] = {"max_overlapping_files": 2}

    # Write #1 — one small file; gate shut (1 < 2).
    dw.write(role_name=ROLE, simple_name=simple, data=_rows(["a", "b", "c"]),
             overwrite_columns=[])
    st1, snap1 = _snapshot(simple)
    files_before = _live_files(snap1)  # {file_A}
    assert len(files_before) == 1
    assert _stats_paths(st1, snap1) == files_before, "stats should describe file_A"

    # Write #2 — gate opens (2 >= 2) → Phase B merges file_A + file_B into one.
    with caplog.at_level(logging.INFO, logger=LOGGER):
        caplog.clear()
        dw.write(role_name=ROLE, simple_name=simple, data=_rows(["d", "e", "f"]),
                 overwrite_columns=[])
        lines = _compaction_lines(caplog)
    assert len(lines) == 1 and "small-file phase merged" in lines[0], lines

    st2, snap2 = _snapshot(simple)
    live_files = _live_files(snap2)
    stats_paths = _stats_paths(st2, snap2)

    # The pre-merge file is sunset...
    assert files_before.isdisjoint(live_files), "merged-away file still listed live"
    # ...and crucially its stats rows are GONE — only the live merged file remains.
    assert files_before.isdisjoint(stats_paths), \
        "sunset file's stats rows survived the merge"
    assert stats_paths == live_files, "stats must describe exactly the live files"
    assert int(snap2.get("stats_rows") or 0) == _stats_df(st2, snap2).height
