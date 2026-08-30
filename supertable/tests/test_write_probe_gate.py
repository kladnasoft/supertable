# supertable/tests/test_write_probe_gate.py
"""Selection tests for overwrite-resolution probe routing.

LocalStorage uses the Island-native scanner automatically because it needs no
httpfs or remote credentials. Remote DuckDB compatibility remains opt-in and
therefore uses the Polars fallback by default. These tests pin the selection
and safety contract:

  * local auto ON -> the probe is called even when the cross-backend flag is OFF;
  * local auto OFF and cross-backend flag OFF -> the Polars fallback is used;
  * cross-backend flag ON -> the probe is called regardless of local auto;
  * a probe failure returns to the strict fallback, including rowid validation;
  * both produce identical (filtered rows, delete pairs) -- the gate changes
    only the mechanism, never the result (the fallback is the semantic oracle).
"""
from __future__ import annotations

import dataclasses
import os
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
def _local_storage(tmp_path):
    """Force both probe and fallback reads through a real LocalStorage so the
    on-disk tmp parquet files are read identically regardless of STORAGE_TYPE."""
    with patch("supertable.processing._get_storage", return_value=LocalStorage(str(tmp_path))):
        yield


def _write(d, name, df):
    path = str(d / name)
    pq.write_table(df.to_arrow(), path)
    return (path, True, (d / name).stat().st_size)


def _auto_candidates(d, *, duplicate_first_rowid: bool = False):
    """Build the non-trivial eight-file set required by the local cost gate."""
    files = set()
    expected_pair = None
    first_key = None
    for index in range(8):
        start = index * 5_000 + 1
        rowids = list(range(start, start + 5_000))
        if index == 0 and duplicate_first_rowid:
            rowids[1] = rowids[0]
        keys = list(range(start, start + 5_000))
        frame = pl.DataFrame({
            "__rowid__": rowids,
            "user_id": keys,
            "updated_at": [7] * len(keys),
        })
        candidate = _write(d, f"candidate-{index}.parquet", frame)
        files.add(candidate)
        if index == 0:
            first_key = start
            expected_pair = (candidate[0], rowids[0])
    projected = st_processing._local_projected_parquet_bytes(
        LocalStorage(str(d)),
        [(path, size) for path, _overlap, size in files],
        ["__rowid__", "user_id", "updated_at"],
    )
    assert projected is not None and projected >= 128 * 1024
    return files, first_key, expected_pair


def _set_probe(monkeypatch, enabled: bool, *, local_auto: bool = False):
    monkeypatch.setattr(
        st_processing, "settings",
        dataclasses.replace(
            settings,
            SUPERTABLE_DUCKDB_WRITE_PROBE=enabled,
            SUPERTABLE_DUCKDB_WRITE_PROBE_LOCAL_AUTO=local_auto,
        ),
    )


def _spy_probe(monkeypatch):
    """Wrap the real local-native probe without altering its behavior."""
    calls = {"n": 0}
    real = st_processing._island_probe_overlap_matches

    def _counting(*a, **k):
        calls["n"] += 1
        return real(*a, **k)

    monkeypatch.setattr(st_processing, "_island_probe_overlap_matches", _counting)
    return calls


def _spy_remote_duckdb(monkeypatch):
    calls = {"n": 0, "kwargs": []}
    real = st_processing._duckdb_probe_overlap_matches

    def _counting(*a, **k):
        calls["n"] += 1
        calls["kwargs"].append(k)
        return real(*a, **k)

    monkeypatch.setattr(st_processing, "_duckdb_probe_overlap_matches", _counting)
    return calls


def _resolve(incoming, files, keys, ntc, prof):
    return resolve_overwrite_writes(
        incoming_df=incoming, overlapping_files=files,
        overwrite_columns=keys, newer_than_col=ntc, profiler=prof,
        storage=st_processing._get_storage(),
    )


def test_local_auto_disabled_uses_fallback(tmp_path, monkeypatch):
    f = _write(tmp_path, "a.parquet", pl.DataFrame(
        {"__rowid__": [1], "user_id": [5], "name": ["Alice"], "updated_at": [7]}))
    incoming = pl.DataFrame({"user_id": [5], "name": ["Bob"], "updated_at": [9]})

    _set_probe(monkeypatch, False, local_auto=False)
    calls = _spy_probe(monkeypatch)
    prof = Profiler()
    filt, pairs = _resolve(incoming, {f}, ["user_id"], "updated_at", prof)

    counts = prof.emit_counts()
    assert calls["n"] == 0, "probe must not run when both selectors are off"
    assert "overwrite_resolve_fallback" in counts, f"fallback not taken; counts={counts}"
    assert "probe_files" not in counts, f"probe ran despite flag off; counts={counts}"
    # Correct result via the fallback: the newer incoming row survives and
    # tombstones the existing row's __rowid__.
    assert filt.height == 1
    assert pairs == [(f[0], 1)]


def test_local_storage_is_automatically_selected(tmp_path, monkeypatch):
    files, key, expected_pair = _auto_candidates(tmp_path)
    incoming = pl.DataFrame({"user_id": [key], "updated_at": [9]})

    _set_probe(monkeypatch, False, local_auto=True)
    calls = _spy_probe(monkeypatch)
    prof = Profiler()
    filt, pairs = _resolve(incoming, files, ["user_id"], "updated_at", prof)

    counts = prof.emit_counts()
    assert calls["n"] == 1, "local auto-selection must call the probe"
    assert "probe_files" in counts, f"probe did not run; counts={counts}"
    assert counts.get("overwrite_resolve_probe_auto_local") == 1
    assert filt.height == 1
    assert pairs == [expected_pair]


def test_local_auto_keeps_small_candidate_set_on_fallback(tmp_path, monkeypatch):
    f = _write(tmp_path, "small.parquet", pl.DataFrame(
        {"__rowid__": [1], "user_id": [5]}))
    incoming = pl.DataFrame({"user_id": [5]})

    _set_probe(monkeypatch, False, local_auto=True)
    calls = _spy_probe(monkeypatch)
    prof = Profiler()
    filt, pairs = _resolve(incoming, {f}, ["user_id"], None, prof)

    assert calls["n"] == 0
    assert prof.emit_counts().get("overwrite_resolve_fallback") == 1
    assert filt.height == 1
    assert pairs == [(f[0], 1)]


def test_local_auto_gate_uses_exact_file_and_projected_byte_boundaries(
        tmp_path, monkeypatch,
):
    storage = LocalStorage(tmp_path)
    seven = [(f"f-{index}.parquet", 10**9) for index in range(7)]
    eight = seven + [("f-7.parquet", 10**9)]

    monkeypatch.setattr(
        st_processing,
        "_local_projected_parquet_bytes",
        lambda *_args: st_processing._LOCAL_WRITE_PROBE_MIN_BYTES,
    )
    assert st_processing._write_probe_selected(
        storage, seven, ["__rowid__", "user_id"],
    ) == (False, False)
    assert st_processing._write_probe_selected(
        storage, eight, ["__rowid__", "user_id"],
    ) == (True, True)

    monkeypatch.setattr(
        st_processing,
        "_local_projected_parquet_bytes",
        lambda *_args: st_processing._LOCAL_WRITE_PROBE_MIN_BYTES - 1,
    )
    assert st_processing._write_probe_selected(
        storage, eight, ["__rowid__", "user_id"],
    ) == (False, False)


def test_unused_payload_bytes_cannot_select_local_probe(tmp_path, monkeypatch):
    storage = LocalStorage(tmp_path)
    files = []
    # The catalog-reported whole-file bytes are deliberately enormous, while
    # the two projected columns contain one tiny row per file. Selection must
    # follow Parquet column chunks, not unrelated payload/catalog width.
    for index in range(8):
        path = tmp_path / f"wide-{index}.parquet"
        pq.write_table(
            pl.DataFrame({
                "__rowid__": [index + 1],
                "user_id": [index + 1],
                "unused_payload": [os.urandom(64 * 1024)],
            }).to_arrow(),
            path,
        )
        files.append((path.name, 10**9))

    projected = st_processing._local_projected_parquet_bytes(
        storage, files, ["__rowid__", "user_id"],
    )
    assert projected is not None
    assert projected < st_processing._LOCAL_WRITE_PROBE_MIN_BYTES
    assert st_processing._write_probe_selected(
        storage, files, ["__rowid__", "user_id"],
    ) == (False, False)


def test_explicit_flag_calls_probe_when_local_auto_is_off(tmp_path, monkeypatch):
    f = _write(tmp_path, "a.parquet", pl.DataFrame(
        {"__rowid__": [1], "user_id": [5], "updated_at": [7]}))
    incoming = pl.DataFrame({"user_id": [5], "updated_at": [9]})

    _set_probe(monkeypatch, True, local_auto=False)
    calls = _spy_probe(monkeypatch)
    prof = Profiler()
    filt, pairs = _resolve(incoming, {f}, ["user_id"], "updated_at", prof)

    assert calls["n"] == 1
    assert prof.emit_counts().get("overwrite_resolve_probe_auto_local") is None
    assert filt.height == 1
    assert pairs == [(f[0], 1)]


def test_explicit_nonlocal_compatibility_uses_duckdb(tmp_path, monkeypatch):
    class _NonlocalReadableStorage(LocalStorage):
        def is_local_storage(self):
            return False

    storage = _NonlocalReadableStorage(tmp_path)
    candidate = _write(tmp_path, "remote-shaped.parquet", pl.DataFrame({
        "__rowid__": [1], "user_id": [5], "updated_at": [7],
    }))
    incoming = pl.DataFrame({"user_id": [5], "updated_at": [9]})
    _set_probe(monkeypatch, True, local_auto=False)
    native_calls = _spy_probe(monkeypatch)
    duckdb_calls = _spy_remote_duckdb(monkeypatch)
    catalog = type("Catalog", (), {
        "get_engine_config": lambda self, org: {
            "duckdb": {"duckdb_threads": "1", "duckdb_memory_limit": "512MB"}
        }
    })()

    from supertable.engine import engine_common
    engine_common.reset_pooled_duckdb_connections()

    try:
        filtered, pairs = resolve_overwrite_writes(
            incoming, {candidate}, ["user_id"], "updated_at", storage=storage,
            organization="org-1", catalog=catalog,
        )

        assert engine_common._probe_pool.con.execute(
            "SELECT current_setting('threads')"
        ).fetchone()[0] == 1
    finally:
        engine_common.reset_pooled_duckdb_connections()

    assert native_calls["n"] == 0
    assert duckdb_calls["n"] == 1
    assert duckdb_calls["kwargs"][0]["organization"] == "org-1"
    assert duckdb_calls["kwargs"][0]["catalog"] is catalog
    assert filtered.rows() == [(5, 9)]
    assert pairs == [(candidate[0], 1)]


def test_nonlocal_storage_is_not_auto_selected(tmp_path, monkeypatch):
    class _NonlocalReadableStorage(LocalStorage):
        def is_local_storage(self):
            return False

    f = _write(tmp_path, "a.parquet", pl.DataFrame(
        {"__rowid__": [1], "user_id": [5]}))
    incoming = pl.DataFrame({"user_id": [5]})

    _set_probe(monkeypatch, False, local_auto=True)
    calls = _spy_probe(monkeypatch)
    with patch(
        "supertable.processing._get_storage",
            return_value=_NonlocalReadableStorage(str(tmp_path)),
    ):
        filt, pairs = _resolve(
            incoming, {f}, ["user_id"], None, Profiler(),
        )

    assert calls["n"] == 0
    assert filt.height == 1
    assert pairs == [(f[0], 1)]


def test_auto_probe_failure_runs_strict_fallback(tmp_path, monkeypatch):
    files, key, expected_pair = _auto_candidates(tmp_path)
    incoming = pl.DataFrame({"user_id": [key], "updated_at": [9]})

    _set_probe(monkeypatch, False, local_auto=True)
    calls = {"n": 0}

    def _unavailable(*args, **kwargs):
        calls["n"] += 1
        return None

    monkeypatch.setattr(
        st_processing, "_island_probe_overlap_matches", _unavailable,
    )
    prof = Profiler()
    filt, pairs = _resolve(incoming, files, ["user_id"], "updated_at", prof)

    assert calls["n"] == 1
    assert prof.emit_counts().get("overwrite_resolve_fallback") == 1
    assert filt.height == 1
    assert pairs == [expected_pair]


def test_probe_failure_fallback_keeps_caller_pinned_storage(
        tmp_path, monkeypatch,
):
    pinned_root = tmp_path / "pinned"
    ambient_root = tmp_path / "ambient"
    pinned_root.mkdir()
    ambient_root.mkdir()
    pinned = LocalStorage(pinned_root)
    ambient = LocalStorage(ambient_root)
    key = "same-key.parquet"
    pq.write_table(
        pl.DataFrame({
            "__rowid__": [1], "user_id": [5], "updated_at": [7],
        }).to_arrow(),
        pinned_root / key,
    )
    # The ambient backend deliberately exposes a conflicting object at the
    # same logical key. A fallback that calls global _get_storage() would read
    # this row and silently return a different mutation decision.
    pq.write_table(
        pl.DataFrame({
            "__rowid__": [99], "user_id": [99], "updated_at": [7],
        }).to_arrow(),
        ambient_root / key,
    )
    candidate = {(key, True, (pinned_root / key).stat().st_size)}
    incoming = pl.DataFrame({"user_id": [5], "updated_at": [9]})

    monkeypatch.setattr(st_processing, "_get_storage", lambda: ambient)
    _set_probe(monkeypatch, True, local_auto=False)
    monkeypatch.setattr(
        st_processing, "_island_probe_overlap_matches", lambda *a, **k: None,
    )
    failed_probe = resolve_overwrite_writes(
        incoming, candidate, ["user_id"], "updated_at", storage=pinned,
    )

    _set_probe(monkeypatch, False, local_auto=False)
    direct_fallback = resolve_overwrite_writes(
        incoming, candidate, ["user_id"], "updated_at", storage=pinned,
    )
    assert failed_probe[0].rows() == direct_fallback[0].rows()
    assert failed_probe[1] == direct_fallback[1]
    assert failed_probe[0].rows() == [(5, 9)]
    assert failed_probe[1] == [(key, 1)]


def test_local_path_resolution_never_presigns(tmp_path, monkeypatch):
    class _PresignTrapLocalStorage(LocalStorage):
        def presign(self, key, expiry_seconds=3600):
            raise AssertionError("local path must never be presigned")

    storage = _PresignTrapLocalStorage(tmp_path)
    monkeypatch.setattr(
        st_processing,
        "settings",
        dataclasses.replace(settings, SUPERTABLE_DUCKDB_PRESIGNED=True),
    )

    assert st_processing._storage_duckdb_path(storage, "data.parquet") == str(
        tmp_path / "data.parquet"
    )


def test_auto_probe_fallback_preserves_rowid_integrity_error(
        tmp_path, monkeypatch,
):
    files, key, _expected_pair = _auto_candidates(
        tmp_path, duplicate_first_rowid=True,
    )
    incoming = pl.DataFrame({"user_id": [key]})

    _set_probe(monkeypatch, False, local_auto=True)
    calls = _spy_probe(monkeypatch)
    prof = Profiler()
    with pytest.raises(ValueError, match="duplicate rowids"):
        _resolve(incoming, files, ["user_id"], None, prof)

    # The native probe rejects the corrupt candidate, then the strict oracle
    # independently proves the same corruption instead of treating probe
    # failure as no match.
    assert calls["n"] == 1
    assert prof.emit_counts().get("overwrite_resolve_fallback") == 1


def test_local_auto_result_is_identical_to_fallback(tmp_path, monkeypatch):
    """Auto-selection changes only the mechanism: rows + delete pairs match.

    user_id=5 is newer (9 > 7) -> survives + tombstones rowid 1; user_id=6 is
    stale (3 < 7) -> dropped, no tombstone.  Identical on both code paths.
    """
    files, key, _expected_pair = _auto_candidates(tmp_path)
    incoming = pl.DataFrame(
        {"user_id": [key, key + 1], "name": ["X", "Y"],
         "updated_at": [9, 3]})

    def _run(*, local_auto):
        _set_probe(monkeypatch, False, local_auto=local_auto)
        filt, pairs = _resolve(
            incoming, files, ["user_id"], "updated_at", Profiler(),
        )
        rows = sorted(
            filt.select(["user_id", "name", "updated_at"]).to_dicts(), key=repr
        )
        return rows, sorted(pairs)

    assert _run(local_auto=True) == _run(local_auto=False)
