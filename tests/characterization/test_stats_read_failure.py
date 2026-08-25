"""Fault-injection probe for the stats carry-forward read (audit Finding #3).

Mechanism under test
--------------------
``build_stats_file`` reads the previous external-stats parquet once to carry it forward
(processing.py:1735):

    prev_df = _read_parquet_safe(prev_stats_path, profiler=p) if prev_stats_path else None
    ...
    else:
        combined = new_df            # prev unreadable -> rebuilt from THIS write only

The hardened path must never publish a replacement stats artifact built from only the
current write when the prior artifact cannot be read.  It either preserves the previous
stats generation or aborts before publishing a partial replacement.  This retains both
correctness and pruning performance across a transient stats-store failure.

What this test does
-------------------
Lands four keys in separate files, injects a failure scoped to the stats object during an
overwrite, and permits either safe policy: complete the write while preserving carried
stats, or abort the write.  It then verifies that stats were not truncated and that a later
delete remains correct.
"""

from __future__ import annotations

import os

import polars as pl
import pyarrow as pa

import supertable.processing as st_processing
from supertable.data_reader import DataReader, engine
from supertable.data_writer import DataWriter
from supertable.simple_table import SimpleTable
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "demo_sdv"
SIMPLE = "sfacts"
ROLE = "superadmin"
KEY = "grp"


def _rows(grps, amounts) -> pa.Table:
    return pa.table({KEY: list(grps), "amount": list(amounts)})


def _live_map() -> dict:
    dr = DataReader(super_name=SUPER, organization=ORG,
                    query=f"SELECT {KEY}, amount FROM {SIMPLE}")
    df, status, msg = dr.execute(role_name=ROLE, with_scan=False, engine=engine.AUTO)
    assert str(status).endswith("OK"), f"read failed: status={status} msg={msg}"
    pdf = df if isinstance(df, pl.DataFrame) else pl.from_pandas(df)
    # amount may be coerced int->float on the read path; int(...) normalises it.
    return {row[0]: int(row[1]) for row in pdf.select([KEY, "amount"]).iter_rows()}


def _stats_rows() -> int:
    st = SimpleTable(SuperTable(SUPER, ORG), SIMPLE)
    snap, _ = st.get_simple_table_snapshot()
    return int(snap.get("stats_rows") or 0)


def _make_stats_read_fail(delegate):
    """Wrap ``storage.read_parquet`` so reads of the ``/stats/`` object raise -- exactly
    as a corrupt/transient object-store read would -- while every other read (data,
    tombstone) is delegated through unchanged."""
    def failing(path, columns=None):
        if f"{os.sep}stats{os.sep}" in path or "/stats/" in path:
            raise RuntimeError(f"injected stats read failure: {path}")
        return delegate(path, columns=columns)
    return failing


def test_stats_read_failure_does_not_drop_live_files_from_delete():
    SuperTable(SUPER, ORG)  # bootstrap super table + default superadmin role/user
    dw = DataWriter(super_name=SUPER, organization=ORG)

    # --- seed: four keys, each landing in its own data file ----------------
    # Separate writes => separate files => four carried-forward stats rows.
    for g, amt in [("a", 1), ("b", 2), ("c", 3), ("d", 4)]:
        dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows([g], [amt]),
                 overwrite_columns=[KEY])
    assert _live_map() == {"a": 1, "b": 2, "c": 3, "d": 4}
    stats_rows_before = _stats_rows()
    assert stats_rows_before > 0  # four files' worth of carried-forward stats

    # --- overwrite "c" while the stats read fails -------------------------
    # Clear the in-process stats cache so both the pruning load AND the build
    # carry-forward read actually hit storage (cold cache == realistic).
    st_processing._STATS_CACHE.clear()
    storage = st_processing._get_storage()
    delegate = storage.read_parquet
    sentinel = object()
    saved = storage.__dict__.get("read_parquet", sentinel)
    storage.read_parquet = _make_stats_read_fail(delegate)
    try:
        # A correctness-preserving fix may ABORT this write; tolerate that.
        try:
            dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["c"], [30]),
                     overwrite_columns=[KEY])
        except Exception:
            pass
    finally:
        if saved is sentinel:
            del storage.read_parquet
        else:
            storage.read_parquet = saved

    # The overwrite itself must be correct regardless of the stats failure:
    # "c" updated to 30, every other key untouched.
    assert _live_map() == {"a": 1, "b": 2, "c": 30, "d": 4}

    # A failed carry-forward read must not publish a truncated replacement.
    # The safe implementation preserves at least the prior generation's rows
    # (and may add rows if it completed the overwrite).
    stats_rows_after = _stats_rows()
    assert stats_rows_after >= stats_rows_before, (
        f"the injected stats-read failure truncated carried stats: "
        f"stats_rows went {stats_rows_before} -> {stats_rows_after}"
    )

    # --- delete "a" after the failed stats read ---------------------------
    # Force the pruner to read the preserved/current artifact from storage.
    st_processing._STATS_CACHE.clear()
    *_, deleted_a = dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["a"], [0]),
                             overwrite_columns=[KEY], delete_only=True)

    live = _live_map()
    assert deleted_a == 1, (
        f"delete of 'a' tombstoned {deleted_a} row(s), expected 1: the candidate file was "
        f"wrongly PRUNED after the failed carry-forward read. live={live}"
    )
    assert "a" not in live, (
        f"key 'a' survived a delete after the failed stats read -- stats-driven "
        f"pruning dropped a live candidate file. live={live}"
    )
    assert live == {"b": 2, "c": 30, "d": 4}
