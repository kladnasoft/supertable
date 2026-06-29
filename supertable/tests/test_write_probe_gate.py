# supertable/tests/test_write_probe_gate.py
"""Gate test for ``SUPERTABLE_DUCKDB_WRITE_PROBE``.

The DuckDB pushdown probe in the overwrite/delete write path is opt-in and
disabled by default.  Environments without the httpfs extension (or without
internet to install it) must NOT stall on a DuckDB httpfs install; they use the
polars fallback, which reads only the projected key columns through the storage
SDK.  These tests pin the gate's contract:

  * flag OFF (default) -> the probe is never called; resolution goes through the
    polars fallback (profiler 'overwrite_resolve_fallback' set, no 'probe_files').
  * flag ON            -> the probe IS called ('probe_files' set).
  * both produce identical (filtered rows, delete pairs) -- the gate changes
    only the mechanism, never the result (the fallback is the semantic oracle).
"""
from __future__ import annotations

import dataclasses
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest

import supertable.processing as st_processing
from supertable.config.settings import settings
from supertable.processing import resolve_overwrite_writes
from supertable.storage.local_storage import LocalStorage
from supertable.utils.profiler import Profiler


@pytest.fixture(autouse=True)
def _local_storage():
    """Force both probe and fallback reads through a real LocalStorage so the
    on-disk tmp parquet files are read identically regardless of STORAGE_TYPE."""
    with patch("supertable.processing._get_storage", return_value=LocalStorage()):
        yield


def _write(d, name, df):
    path = str(d / name)
    pq.write_table(df.to_arrow(), path)
    return (path, True, (d / name).stat().st_size)


def _set_probe(monkeypatch, enabled: bool):
    monkeypatch.setattr(
        st_processing, "settings",
        dataclasses.replace(settings, SUPERTABLE_DUCKDB_WRITE_PROBE=enabled),
    )


def _spy_probe(monkeypatch):
    """Wrap the real probe to count calls without altering its behavior."""
    calls = {"n": 0}
    real = st_processing._duckdb_probe_overlap_matches

    def _counting(*a, **k):
        calls["n"] += 1
        return real(*a, **k)

    monkeypatch.setattr(st_processing, "_duckdb_probe_overlap_matches", _counting)
    return calls


def _resolve(incoming, files, keys, ntc, prof):
    return resolve_overwrite_writes(
        incoming_df=incoming, overlapping_files=files,
        overwrite_columns=keys, newer_than_col=ntc, profiler=prof,
    )


def test_probe_disabled_by_default_uses_fallback(tmp_path, monkeypatch):
    f = _write(tmp_path, "a.parquet", pl.DataFrame(
        {"__rowid__": [1], "user_id": [5], "name": ["Alice"], "updated_at": [7]}))
    incoming = pl.DataFrame({"user_id": [5], "name": ["Bob"], "updated_at": [9]})

    _set_probe(monkeypatch, False)
    calls = _spy_probe(monkeypatch)
    prof = Profiler()
    filt, pairs = _resolve(incoming, {f}, ["user_id"], "updated_at", prof)

    counts = prof.emit_counts()
    assert calls["n"] == 0, "probe must NOT be called when the flag is off"
    assert "overwrite_resolve_fallback" in counts, f"fallback not taken; counts={counts}"
    assert "probe_files" not in counts, f"probe ran despite flag off; counts={counts}"
    # Correct result via the fallback: the newer incoming row survives and
    # tombstones the existing row's __rowid__.
    assert filt.height == 1
    assert pairs == [(f[0], 1)]


def test_probe_enabled_calls_probe(tmp_path, monkeypatch):
    f = _write(tmp_path, "a.parquet", pl.DataFrame(
        {"__rowid__": [1], "user_id": [5], "name": ["Alice"], "updated_at": [7]}))
    incoming = pl.DataFrame({"user_id": [5], "name": ["Bob"], "updated_at": [9]})

    _set_probe(monkeypatch, True)
    calls = _spy_probe(monkeypatch)
    prof = Profiler()
    filt, pairs = _resolve(incoming, {f}, ["user_id"], "updated_at", prof)

    counts = prof.emit_counts()
    assert calls["n"] == 1, "probe must be called when the flag is on"
    assert "probe_files" in counts, f"probe did not run; counts={counts}"
    assert filt.height == 1
    assert pairs == [(f[0], 1)]


def test_gate_result_identical_on_and_off(tmp_path, monkeypatch):
    """The flag changes only the mechanism: filtered rows + delete pairs match.

    user_id=5 is newer (9 > 7) -> survives + tombstones rowid 1; user_id=6 is
    stale (3 < 7) -> dropped, no tombstone.  Identical on both code paths.
    """
    f = _write(tmp_path, "d.parquet", pl.DataFrame(
        {"__rowid__": [1, 2], "user_id": [5, 6], "name": ["A", "B"],
         "updated_at": [7, 7]}))
    incoming = pl.DataFrame(
        {"user_id": [5, 6], "name": ["X", "Y"], "updated_at": [9, 3]})

    def _run(enabled):
        _set_probe(monkeypatch, enabled)
        filt, pairs = _resolve(incoming, {f}, ["user_id"], "updated_at", Profiler())
        rows = sorted(
            filt.select(["user_id", "name", "updated_at"]).to_dicts(), key=repr
        )
        return rows, sorted(pairs)

    assert _run(True) == _run(False)
