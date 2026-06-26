# supertable/engine/tests/test_engine_spill.py
"""End-to-end audit: does the real DuckDB connection adopt the engine config
and actually spill to the configured temp directory?

These tests replicate the exact runtime sequence the executor drives:

    con = duckdb.connect()                      # in-memory, like Lite/Pro
    init_connection(con, temp_dir=<abs path>)   # pragmas at connect time
    apply_runtime_pragmas(con, engine_config)   # live per-query config

and then (a) read settings back off the *same* connection and (b) run a
query whose working set far exceeds the memory limit, proving DuckDB spills
to disk in the configured directory instead of raising OutOfMemory.
"""

from __future__ import annotations

import os

import duckdb
import pytest

from supertable.engine.engine_common import init_connection, apply_runtime_pragmas
from supertable.engine.engine_config import EngineRuntimeConfig


def _cfg(mem, threads=None, cache=""):
    return EngineRuntimeConfig(
        engine_lite_max_bytes=100 * 1024 * 1024,
        engine_spark_min_bytes=10 * 1024 * 1024 * 1024,
        engine_freshness_sec=300,
        duckdb_memory_limit=mem,
        duckdb_io_multiplier=3.0,
        duckdb_threads=threads,
        duckdb_http_timeout=None,
        duckdb_external_cache_size=cache,
    )


def _human_to_bytes(s: str) -> float:
    """Parse DuckDB's current_setting('memory_limit') human string to bytes."""
    s = s.strip()
    units = {"KIB": 1024, "MIB": 1024**2, "GIB": 1024**3, "TIB": 1024**4,
             "KB": 1000, "MB": 1000**2, "GB": 1000**3, "TB": 1000**4, "BYTES": 1, "B": 1}
    num, unit = s.split()
    return float(num) * units[unit.upper()]


# --------------------------------------------------------------------------- #
# 1. Config is actually adopted by the real connection
# --------------------------------------------------------------------------- #

class TestConfigAdopted:
    def test_resolved_memory_limit_lands_on_connection(self, tmp_path):
        con = duckdb.connect()
        init_connection(con, temp_dir=str(tmp_path))           # init default
        apply_runtime_pragmas(con, _cfg("8GB", threads=8))     # Pro cfg
        mem = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        # 8 GB decimal == 7.45 GiB; assert it tracks the cfg, not 80%-of-RAM.
        assert 7.0e9 <= _human_to_bytes(mem) <= 8.1e9, mem

    def test_bare_unitless_value_is_defended_at_pragma_time(self, tmp_path):
        # Even if a bare "2" reaches apply_runtime_pragmas (old data / direct
        # call), normalize_memory_size inside it must coerce to 2GB — never a
        # ParserException, never silently the wrong limit.
        con = duckdb.connect()
        init_connection(con, temp_dir=str(tmp_path))
        apply_runtime_pragmas(con, _cfg("2"))                  # <-- bare number
        mem = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        assert 1.7e9 <= _human_to_bytes(mem) <= 2.1e9, mem

    def test_threads_track_config(self, tmp_path):
        con = duckdb.connect()
        init_connection(con, temp_dir=str(tmp_path))
        apply_runtime_pragmas(con, _cfg("4GB", threads=6))
        threads = con.execute("SELECT current_setting('threads')").fetchone()[0]
        assert int(threads) == 6

    def test_temp_directory_is_absolute_configured_path(self, tmp_path):
        con = duckdb.connect()
        init_connection(con, temp_dir=str(tmp_path))
        td = con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
        assert td == str(tmp_path)
        assert os.path.isabs(td)
        assert td != ".tmp"


# --------------------------------------------------------------------------- #
# 2. Spilling actually happens, in the configured directory
# --------------------------------------------------------------------------- #

# A working set well beyond the memory limit so a spillable operator (ORDER BY)
# is forced to disk.  5M rows * ~120 bytes ~= 600 MB >> 256 MB limit.
_SPILL_SQL = (
    "SELECT i, repeat('x', 100) AS pad "
    "FROM range(5000000) t(i) "
    "ORDER BY i DESC"
)


def _run_heavy(memory_limit: str, temp_directory: str):
    """Run the spill-forcing query under an explicit (memory_limit, temp_directory).

    Returns None on success, or the raised exception.  A fresh connection per
    call keeps the cases independent.
    """
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET threads=4;")
    con.execute(f"PRAGMA memory_limit='{memory_limit}';")
    con.execute(f"PRAGMA temp_directory='{temp_directory}';")
    try:
        con.execute(_SPILL_SQL).fetchone()
        return None
    except Exception as e:  # noqa: BLE001 - the test inspects the exception
        return e
    finally:
        con.close()


def test_spill_is_real_and_targets_the_configured_dir(tmp_path):
    """Causal proof that the heavy query spills, and spills into *our* dir.

    DuckDB's temp blocks are not observable via directory listing,
    ``/proc/<pid>/fd`` or ``duckdb_temporary_files()`` on this build (it
    create-unlinks / mmaps them), so rather than race to catch ephemeral
    files we prove spilling by its *effect* with a truth table:

        memory   temp_directory     outcome
        -------  -----------------  -------
        256 MB   writable           success  (spilled to disk)
        256 MB   uncreatable path   FAIL     (must spill, cannot)
        4 GB     uncreatable path   success  (never needs temp at all)

    Rows 2 and 3 share the *same* broken temp path; only the memory limit
    differs.  So the row-2 failure is caused specifically by the need to spill
    — not by eager temp validation — and the failure names the configured
    path, proving the spill targets the directory we set.
    """
    good = tmp_path / "good"
    blocker = tmp_path / "iam_a_file"
    blocker.write_text("not a directory")
    bad = f"{blocker}{os.sep}nope"  # a path *under* a regular file: uncreatable

    # Row 1: writable temp + tight memory -> spills, succeeds.
    assert _run_heavy("256MB", str(good)) is None

    # Row 2: same tight memory, but spilling cannot write -> must fail.
    err = _run_heavy("256MB", bad)
    assert err is not None, "query did not fail though it had to spill to an uncreatable temp dir"
    assert isinstance(err, duckdb.Error)
    # The failure names the configured temp path: the spill targeted *our* dir.
    assert str(blocker) in str(err), str(err)

    # Row 3: same broken temp path, but ample memory -> no spill, succeeds.
    # The disambiguator: it proves row 2 failed because of spilling, not
    # because DuckDB eagerly validates temp_directory.
    assert _run_heavy("4GB", bad) is None


def test_query_completes_under_tiny_limit_because_it_spills(tmp_path):
    """The same heavy query must NOT raise OutOfMemory when temp is configured."""
    con = duckdb.connect()
    init_connection(con, temp_dir=str(tmp_path), memory_limit="256MB")
    # If spilling were broken this raises duckdb.OutOfMemoryException.
    out = con.execute(_SPILL_SQL).fetchone()
    assert out is not None


def test_negative_control_unwritable_temp_breaks_spill(tmp_path):
    """Point temp_directory at a non-directory so spilling cannot write; the
    heavy query must then fail — proving the spill genuinely depends on the
    configured temp_directory (i.e. it is honoured, not incidental)."""
    blocker = tmp_path / "iam_a_file"
    blocker.write_text("not a directory")
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='256MB';")
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET threads=1;")
    # A path *under* a regular file cannot be created/written.
    con.execute(f"PRAGMA temp_directory='{blocker}{os.sep}nope';")
    with pytest.raises(duckdb.Error):
        con.execute(_SPILL_SQL).fetchone()
