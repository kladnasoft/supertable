"""§7 probe/fallback equivalence under NULL overwrite-keys (audit Finding #4, SUSPECTED).

The finding
-----------
Finding #4 (SUSPECTED, Medium/Low) claimed the DuckDB write-probe and the polars
fallback DIVERGE for NULL overwrite-keys under ``newer_than``: that the probe matches a
NULL key null-safely (``IS NOT DISTINCT FROM``, processing.py:1129) and may drop the
incoming row as stale, while the fallback's ``filter_stale_incoming_rows`` uses a plain
``how="left"`` join (processing.py:823, NULL != NULL) and KEEPS it -- so the two paths
would tombstone / keep different rows for the same input.

What this test does (exactly what §7 asks: "force both paths on the same input, diff")
--------------------------------------------------------------------------------------
Seeds existing data containing a NULL-keyed row (via a plain append, so the NULL lands
verbatim), then runs ``resolve_overwrite_writes`` over the SAME on-disk files + incoming
frame TWICE: once normally (DuckDB probe) and once with ``_duckdb_probe_overlap_matches``
monkeypatched to ``None`` (forced polars fallback).  It diffs the surviving rows and the
``(file, __rowid__)`` delete-pairs.  A guard first confirms the probe genuinely engages,
so the comparison is probe-vs-fallback, never a vacuous fallback-vs-fallback.

Empirical result: they AGREE -> Finding #4 (probe/fallback divergence) is NOT real.
Both the probe stale filter (``_derive_stale_and_deletes``) and the fallback
(``filter_stale_incoming_rows``) share the SAME join, so they never diverge from each
other -- that equivalence is this test's primary, permanent guarantee.

The real (path-independent) issue this test pinned down -- NOW FIXED (audit R7)
------------------------------------------------------------------------------
Originally BOTH paths used a null-UNSAFE ``how="left"`` stale-filter join, so a NULL
overwrite-key bypassed the ``newer_than`` filter yet was still matched null-SAFELY on
the DELETE side (``nulls_equal=True``).  Net effect of an overwrite+newer_than whose key
is NULL: the OLDER incoming row survived and tombstoned the *newer* existing NULL-keyed
row -- a newer_than violation (identical on both paths, so never a probe/fallback
divergence).  R7 made BOTH stale-filter joins ``nulls_equal=True`` (processing.py:823 /
1267), consistent with the delete semi-join (955 / 1280): a NULL key now compares against
the existing NULL group's max, so a stale NULL-keyed row is dropped and the newer existing
NULL row is preserved.  The ground-truth assertions below now verify that fixed behavior;
the probe-vs-fallback equivalence assertions are unchanged.
"""

from __future__ import annotations

import collections
import dataclasses

import polars as pl
import pyarrow as pa
import pytest

import supertable.processing as st_processing
from supertable.config.settings import settings
from supertable.data_writer import DataWriter
from supertable.simple_table import SimpleTable
from supertable.super_table import SuperTable


@pytest.fixture(autouse=True)
def _enable_write_probe(monkeypatch):
    """The DuckDB pushdown probe is opt-in (``SUPERTABLE_DUCKDB_WRITE_PROBE``,
    default off).  Force it on so ``_resolve`` (which goes through
    ``resolve_overwrite_writes``) genuinely exercises the probe — otherwise the
    probe-vs-fallback comparison would degrade to fallback-vs-fallback."""
    monkeypatch.setattr(
        st_processing, "settings",
        dataclasses.replace(settings, SUPERTABLE_DUCKDB_WRITE_PROBE=True),
    )

ORG = "kladna-soft"
SUPER = "demo_pf"
SIMPLE = "pffacts"
ROLE = "superadmin"
KEY = "grp"
VER = "ver"


def _seed_existing(dw, keys, vers) -> None:
    """Land the existing rows verbatim via a plain append (overwrite_columns=[]),
    so a NULL key is stored as-is and each row gets a real ``__rowid__``."""
    dw.write(role_name=ROLE, simple_name=SIMPLE,
             data=pa.table({KEY: list(keys), VER: list(vers)}),
             overwrite_columns=[])


def _overlapping_files() -> set:
    st = SimpleTable(SuperTable(SUPER, ORG), SIMPLE)
    snap, _ = st.get_simple_table_snapshot()
    return {
        (r["file"], True, int(r.get("file_size") or 0))
        for r in (snap.get("resources") or [])
        if isinstance(r, dict) and r.get("file")
    }


def _resolve(incoming, overlapping, newer_than):
    return st_processing.resolve_overwrite_writes(
        incoming_df=incoming,
        overlapping_files=overlapping,
        overwrite_columns=[KEY],
        newer_than_col=newer_than,
    )


def _resolve_forced_fallback(incoming, overlapping, newer_than):
    """Drive the SAME call with the DuckDB probe disabled -> polars oracle path."""
    saved = st_processing._duckdb_probe_overlap_matches
    st_processing._duckdb_probe_overlap_matches = lambda *a, **k: None
    try:
        return _resolve(incoming, overlapping, newer_than)
    finally:
        st_processing._duckdb_probe_overlap_matches = saved


def _rowset(df) -> collections.Counter:
    """Order-independent multiset of (key, ver) rows; tolerates NULL keys."""
    return collections.Counter(tuple(r) for r in df.select([KEY, VER]).iter_rows())


def _assert_probe_engages(incoming, overlapping, newer_than):
    """Guard: confirm the DuckDB probe really matches in-harness, so the equivalence
    below is probe-vs-fallback and not a vacuous fallback-vs-fallback."""
    overlap_true = [(f, sz) for f, ov, sz in overlapping if ov]
    incoming_keys = incoming.select([KEY]).unique()
    matched = st_processing._duckdb_probe_overlap_matches(
        overlap_true, [KEY], newer_than, incoming_keys,
    )
    assert matched is not None and matched.height > 0, (
        "the DuckDB probe did not engage for this input in-harness; the equivalence "
        "assertions would be vacuous (comparing the fallback against itself)"
    )


def test_probe_and_fallback_agree_on_null_key_under_newer_than():
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)

    # Existing: non-null key "x" at ver=10, and a NULL key at ver=10.
    _seed_existing(dw, ["x", None], [10, 10])
    overlapping = _overlapping_files()
    assert overlapping, "seed produced no resources"

    # Incoming: SAME keys at an OLDER ver (5 < 10).  A *null-safe* stale filter would
    # drop BOTH as stale; a *null-unsafe* one keeps the NULL-keyed row.  This is the
    # exact input that would expose the divergence Finding #4 alleged.
    incoming = pl.DataFrame({KEY: ["x", None], VER: [5, 5]})

    _assert_probe_engages(incoming, overlapping, VER)

    filt_probe, pairs_probe = _resolve(incoming, overlapping, VER)
    filt_fb, pairs_fb = _resolve_forced_fallback(incoming, overlapping, VER)

    # PRIMARY -- the two paths are equivalent => Finding #4 (divergence) is NOT real.
    assert _rowset(filt_probe) == _rowset(filt_fb), (
        f"probe vs fallback DIVERGED on surviving rows (Finding #4 would be REAL): "
        f"probe={dict(_rowset(filt_probe))} fallback={dict(_rowset(filt_fb))}"
    )
    assert sorted(pairs_probe) == sorted(pairs_fb), (
        f"probe vs fallback DIVERGED on delete pairs (Finding #4 would be REAL): "
        f"probe={sorted(pairs_probe)} fallback={sorted(pairs_fb)}"
    )

    # GROUND TRUTH both paths agree on (post-R7, null-safe stale filter): BOTH incoming
    # rows are stale -- non-null "x" (5 <= existing 10) AND the NULL key (5 <= the
    # existing NULL-group max 10) -- so NOTHING survives the stale filter.
    survivors = _rowset(filt_probe)
    assert survivors == collections.Counter(), (
        f"expected NO incoming row to survive the stale filter on BOTH paths (both 'x' "
        f"and the NULL key are older than the existing ver=10); got {dict(survivors)}"
    )
    # ...and with no surviving key, nothing is tombstoned: the newer existing NULL-keyed
    # row is preserved -- newer_than is now honored for NULL keys (R7 fix), no longer
    # clobbered by an older incoming NULL-keyed row.
    assert len(pairs_probe) == 0, (
        f"expected no tombstones: the stale NULL-keyed incoming row must not replace the "
        f"newer existing NULL row; pairs={pairs_probe}"
    )


def test_probe_and_fallback_agree_on_null_key_overwrite_without_newer_than():
    """Delete-pair half (which the audit said was already equivalent): a plain overwrite
    of a NULL key must tombstone the existing NULL-keyed row identically on both paths."""
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)

    _seed_existing(dw, ["x", None], [10, 10])
    overlapping = _overlapping_files()
    assert overlapping, "seed produced no resources"

    incoming = pl.DataFrame({KEY: ["x", None], VER: [1, 1]})

    _assert_probe_engages(incoming, overlapping, None)

    filt_probe, pairs_probe = _resolve(incoming, overlapping, None)
    filt_fb, pairs_fb = _resolve_forced_fallback(incoming, overlapping, None)

    # No newer_than => no stale filter => every incoming row survives on both paths.
    assert _rowset(filt_probe) == _rowset(filt_fb) == _rowset(incoming)
    # Both tombstone BOTH existing rows including the NULL-keyed one (delete side is
    # null-safe on both paths) => delete-pair equivalence holds, NULL keys included.
    assert sorted(pairs_probe) == sorted(pairs_fb)
    assert len(pairs_probe) == 2, (
        f"expected both existing rows (incl. the NULL key) tombstoned; pairs={pairs_probe}"
    )
