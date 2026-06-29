"""Fault-injection probe for the stats carry-forward read (audit Finding #3).

Mechanism under test
--------------------
``build_stats_file`` reads the previous external-stats parquet once to carry it forward
(processing.py:1735):

    prev_df = _read_parquet_safe(prev_stats_path, profiler=p) if prev_stats_path else None
    ...
    else:
        combined = new_df            # prev unreadable -> rebuilt from THIS write only

On the LENIENT default path a failed read returns ``None``, so the new stats artifact is
rebuilt from only the current write's rows -- every carried-forward per-file stat is
dropped.  The sole consumer is stats-driven candidate pruning for overwrite/delete
(data_writer.py:392-425 -> ``prune_overlapping_files_by_stats``), whose contract is
one-directional (processing.py:1761-1770): every uncertainty, including a *missing* per-file
stat, resolves to RETAIN, never to DROP.  A snapshot whose ``stats_file`` is empty simply
retains all candidates (data_writer.py:417-420).

So a stats-read failure can only cause MORE files to be scanned, never fewer -- query and
delete results stay identical, only slower.

What this test does
-------------------
Lands four keys in four separate data files (so there are carried-forward stats to lose),
then overwrites one key while a read failure is injected *scoped to the stats object only*.
That truncates the stats artifact down to just the newly-written file -- the other three
files are now statless.  It then deletes a key that lives in one of those statless files
(no injection) and asserts the delete actually took effect: ``deleted == 1`` and the key is
gone.  Had the pruner treated "no stat entry" as prunable, that file would be dropped from
the candidate set, the delete would tombstone nothing (``deleted == 0``), and the row would
survive.

Decision: ACCEPTED, not fixed
-----------------------------
Unlike the deletion-vector reads (Findings #1 and #2), this carry-forward read is left
LENIENT on purpose.  Making it ``required=True`` would abort a user *data write* on a transient
stats-read failure -- trading a mere scan slowdown for an availability hit, when the written
data lands correctly either way and the only cost is reduced pruning.  So Finding #3 is a
documented, accepted degradation; this test seals that behavior.

Expected to PASS, documenting Finding #3 as an accepted pruning/perf degradation.  (A failure
would instead mean missing stats can DROP live files from the delete candidate set -- a real
correctness defect that WOULD force this read to be made required, like the DV reads.)
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

    # --- overwrite "c" while the stats read fails -> truncates carried stats
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

    # Prove the injection actually bit: with the carry-forward read failing,
    # build_stats_file rebuilt the artifact from only the just-written "c" file
    # (combined = new_df), dropping a/b/d's carried stats -- so the snapshot's
    # stats_rows is now strictly smaller.  Without this, the delete assertions
    # below could pass trivially (files still had stats) instead of exercising the
    # retain-statless-files pruning path that is the whole point of the test.
    stats_rows_after = _stats_rows()
    assert stats_rows_after < stats_rows_before, (
        f"expected the injected stats-read failure to TRUNCATE the carried-forward "
        f"stats, but stats_rows went {stats_rows_before} -> {stats_rows_after}; the "
        f"delete assertions below would then pass trivially without ever exercising "
        f"the missing-stat pruning path"
    )

    # --- delete "a" (its file's stats were just truncated away) ------------
    # Force the pruner to read the truncated stats artifact from storage.
    st_processing._STATS_CACHE.clear()
    *_, deleted_a = dw.write(role_name=ROLE, simple_name=SIMPLE, data=_rows(["a"], [0]),
                             overwrite_columns=[KEY], delete_only=True)

    live = _live_map()
    assert deleted_a == 1, (
        f"delete of 'a' tombstoned {deleted_a} row(s), expected 1: the candidate file was "
        f"wrongly PRUNED because its stats entry was dropped by the failed carry-forward "
        f"read. The pruner must RETAIN files with no stat entry. live={live}"
    )
    assert "a" not in live, (
        f"key 'a' survived a delete after its file's stats were truncated -- stats-driven "
        f"pruning dropped a live candidate file. live={live}"
    )
    assert live == {"b": 2, "c": 30, "d": 4}
