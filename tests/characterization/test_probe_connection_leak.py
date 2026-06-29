"""§8 leak check: the transient write-probe DuckDB connection is closed on EVERY path.

The §8 question
---------------
``_duckdb_probe_overlap_matches`` (processing.py:1068) spins up a *transient*
DuckDB connection per overwrite to do the key-overlap semi-join.  §8 asks: is
that connection closed on ALL paths, including exceptions?  A leaked connection
per failed probe is a real (slow-burn) resource leak.

The code's claim
----------------
The connection is created as ``con = None`` then built inside a ``try`` whose
``finally`` unconditionally unregisters the incoming-keys relation and calls
``con.close()`` (processing.py:1149-1186).  So whether the scan succeeds, raises
a presign-retryable error, or raises anything else, the ``finally`` should run
and close the connection.

What this test does -- drives the real function, not a re-implementation
-----------------------------------------------------------------------
``new_duckdb_connection`` is imported *inside* the probe from
``engine.engine_common``; patching it on that module is picked up at call time.
Two cases, both calling the real ``_duckdb_probe_overlap_matches`` over a real
seeded data file:

  * FAILURE path -- patch the factory to hand back a fake connection whose
    ``execute`` raises a NON-presign error.  The probe must swallow it and
    return ``None`` (fall back to polars), and -- the point of the test -- the
    fake's ``close()`` must have been called by the ``finally``.  If the
    ``finally`` were missing, the connection would leak on every failed probe.

  * SUCCESS path -- wrap the REAL connection in a spy that delegates everything
    but records ``close()``.  A genuine overlapping key makes the probe match
    and return a non-empty frame; the spy must still have been closed.  Proves
    the happy path doesn't leak either.

Expected: PASS on both -- the try/finally closes the connection on success and on
failure alike, so §8 (probe connection leak) is NOT a real bug.  Remove the
``finally``'s ``con.close()`` and the failure case's ``closed`` assertion flips red.
"""

from __future__ import annotations

import polars as pl
import pyarrow as pa

import supertable.processing as st_processing
from supertable.engine import engine_common
from supertable.data_writer import DataWriter
from supertable.simple_table import SimpleTable
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "demo_leak"
SIMPLE = "lkfacts"
ROLE = "superadmin"
KEY = "grp"


def _seed_one_file(dw) -> None:
    dw.write(role_name=ROLE, simple_name=SIMPLE, data=pa.table({KEY: ["x"], "amount": [1]}),
             overwrite_columns=[KEY])


def _overlap_true_files() -> list:
    """Real ``[(file_key, file_size)]`` for the probe's ``overlap_true_files`` arg."""
    st = SimpleTable(SuperTable(SUPER, ORG), SIMPLE)
    snap, _ = st.get_simple_table_snapshot()
    return [
        (r["file"], int(r.get("file_size") or 0))
        for r in (snap.get("resources") or [])
        if isinstance(r, dict) and r.get("file")
    ]


class _FakeConn:
    """Stand-in connection whose scan raises -- to prove the finally still closes it."""
    def __init__(self) -> None:
        self.closed = False
    def register(self, name, obj) -> None:  # noqa: D401 - probe registers incoming keys
        pass
    def execute(self, sql):
        # A non-presign message so the probe re-raises straight to its outer
        # handler (return None) instead of attempting the presign retry branch.
        raise RuntimeError("injected probe failure: forced DuckDB execute error")
    def unregister(self, name) -> None:
        pass
    def close(self) -> None:
        self.closed = True


class _SpyConn:
    """Delegating proxy around the REAL connection that records close()."""
    def __init__(self, real) -> None:
        object.__setattr__(self, "_real", real)
        object.__setattr__(self, "closed", False)
    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_real"), name)
    def close(self):
        object.__setattr__(self, "closed", True)
        return object.__getattribute__(self, "_real").close()


def test_probe_failure_closes_connection_no_leak():
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)
    _seed_one_file(dw)

    overlap = _overlap_true_files()
    assert overlap, "seed produced no resource file for the probe to scan"
    incoming_keys = pl.DataFrame({KEY: ["x"]}).select([KEY]).unique()

    fake = _FakeConn()
    saved = engine_common.new_duckdb_connection
    engine_common.new_duckdb_connection = lambda *a, **k: fake
    try:
        result = st_processing._duckdb_probe_overlap_matches(
            overlap, [KEY], None, incoming_keys,
        )
    finally:
        engine_common.new_duckdb_connection = saved

    # The probe swallowed the scan error and signalled "fall back to polars".
    assert result is None, "a failed scan must return None so the caller falls back"
    # ...and -- the actual leak check -- the finally closed the connection.
    assert fake.closed is True, (
        "the transient DuckDB connection was NOT closed after the probe scan raised "
        "-- every failed probe would leak a connection (missing try/finally close)"
    )


def test_probe_success_closes_connection_no_leak():
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)
    _seed_one_file(dw)

    overlap = _overlap_true_files()
    assert overlap, "seed produced no resource file for the probe to scan"
    incoming_keys = pl.DataFrame({KEY: ["x"]}).select([KEY]).unique()  # overlaps 'x'

    spies = []
    saved = engine_common.new_duckdb_connection

    def _spying_factory(*a, **k):
        spy = _SpyConn(saved(*a, **k))
        spies.append(spy)
        return spy

    engine_common.new_duckdb_connection = _spying_factory
    try:
        result = st_processing._duckdb_probe_overlap_matches(
            overlap, [KEY], None, incoming_keys,
        )
    finally:
        engine_common.new_duckdb_connection = saved

    # Guard: the probe genuinely engaged (matched the seeded 'x'), so this is a
    # real success path and not a vacuous early-return before the connection.
    assert result is not None and result.height > 0, (
        "probe did not engage in-harness; the success-path close assertion would be vacuous"
    )
    assert spies, "the patched connection factory was never called"
    assert all(s.closed for s in spies), (
        "the transient DuckDB connection was not closed on the successful probe path"
    )
