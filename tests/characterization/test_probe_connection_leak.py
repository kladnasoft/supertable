"""§8 leak check: the pooled write-probe DuckDB connection is bounded, not leaked.

The §8 question
---------------
``_duckdb_probe_overlap_matches`` runs the key-overlap semi-join on a DuckDB
connection per overwrite.  §8 asks: can that connection leak — one abandoned per
probe, especially on the failure path?

The code's claim (post-pooling)
-------------------------------
The probe no longer builds a *transient* connection it must close.  It calls
``get_pooled_duckdb_connection``, which keeps ONE connection per thread in a
``threading.local`` pool and reuses it across writes (amortising the ~150 ms
warmup).  So the probe's ``finally`` only unregisters the per-probe incoming-keys
relation — it deliberately does NOT close the connection, which is returned to
the pool.  The no-leak guarantee is therefore: N probes (success or failure)
share ONE connection, never N; the per-probe relation is unregistered so it
can't accumulate; and ``reset_pooled_duckdb_connections()`` closes the pooled
connection, bounding the resource.

What this test does -- drives the real function, not a re-implementation
-----------------------------------------------------------------------
``new_duckdb_connection`` is the cold-build factory ``get_pooled_duckdb_connection``
calls; patching it on ``engine_common`` is picked up at call time.  Two cases,
both calling the real ``_duckdb_probe_overlap_matches`` twice over a real seeded
data file (the autouse ``_reset_probe_pool`` fixture gives each a cold pool):

  * FAILURE path -- factory hands back a fake whose ``execute`` raises a
    NON-presign error.  Both probes must return ``None`` (fall back to polars),
    the factory must run ONCE (the failed probe reuses the pooled connection,
    not one-per-failure), the probe must NOT close it, and the reset hook must.

  * SUCCESS path -- wrap the REAL connection in a spy.  A genuine overlapping
    key makes both probes match; the connection is built once, reused on the
    warm probe, never closed by the probe, and closed by the reset hook.

Expected: PASS on both -- pooling bounds the connection to one-per-thread and the
reset hook closes it, so §8 (probe connection leak) is NOT a real bug.
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


def test_probe_failure_pools_connection_no_leak():
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)
    _seed_one_file(dw)

    overlap = _overlap_true_files()
    assert overlap, "seed produced no resource file for the probe to scan"
    incoming_keys = pl.DataFrame({KEY: ["x"]}).select([KEY]).unique()

    engine_common.reset_pooled_duckdb_connections()  # start cold
    fake = _FakeConn()
    builds = []
    saved = engine_common.new_duckdb_connection
    engine_common.new_duckdb_connection = lambda *a, **k: (builds.append(1), fake)[1]
    try:
        r1 = st_processing._duckdb_probe_overlap_matches(overlap, [KEY], None, incoming_keys)
        r2 = st_processing._duckdb_probe_overlap_matches(overlap, [KEY], None, incoming_keys)
    finally:
        engine_common.new_duckdb_connection = saved

    # Both failed scans signalled "fall back to polars".
    assert r1 is None and r2 is None, "a failed scan must return None so the caller falls back"
    # The connection is built ONCE and reused -- not one abandoned per failed probe.
    assert len(builds) == 1, "failed probe rebuilt the connection instead of reusing the pool"
    # The probe must NOT close the pooled connection (it is returned for reuse)...
    assert fake.closed is False, "probe closed the pooled connection -- breaks reuse"
    assert engine_common._probe_pool.con is fake, "failed probe did not retain the pooled connection"
    # ...and the reset hook closes it, bounding the resource (this is the no-leak guarantee).
    engine_common.reset_pooled_duckdb_connections()
    assert fake.closed is True, "reset hook did not close the pooled connection -- would leak"
    assert getattr(engine_common._probe_pool, "con", None) is None


def test_probe_success_pools_and_reuses_connection_no_leak():
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)
    _seed_one_file(dw)

    overlap = _overlap_true_files()
    assert overlap, "seed produced no resource file for the probe to scan"
    incoming_keys = pl.DataFrame({KEY: ["x"]}).select([KEY]).unique()  # overlaps 'x'

    engine_common.reset_pooled_duckdb_connections()  # start cold
    spies = []
    saved = engine_common.new_duckdb_connection

    def _spying_factory(*a, **k):
        spy = _SpyConn(saved(*a, **k))
        spies.append(spy)
        return spy

    engine_common.new_duckdb_connection = _spying_factory
    try:
        r1 = st_processing._duckdb_probe_overlap_matches(overlap, [KEY], None, incoming_keys)
        r2 = st_processing._duckdb_probe_overlap_matches(overlap, [KEY], None, incoming_keys)
    finally:
        engine_common.new_duckdb_connection = saved

    # Guard: the probe genuinely engaged (matched the seeded 'x') on both calls,
    # so these are real success paths and not a vacuous early-return.
    assert r1 is not None and r1.height > 0, "first probe did not engage in-harness"
    assert r2 is not None and r2.height > 0, "second probe did not engage in-harness"
    # Built ONCE and reused on the warm probe -- one pooled connection per thread.
    assert len(spies) == 1, "warm probe rebuilt the connection instead of reusing the pool"
    # The happy path does NOT close it (returned to the pool for reuse)...
    assert spies[0].closed is False, "happy path closed the pooled connection -- breaks reuse"
    assert engine_common._probe_pool.con is spies[0], "probe did not retain the pooled connection"
    # ...and the reset hook closes the pooled connection, so it is bounded, not leaked.
    engine_common.reset_pooled_duckdb_connections()
    assert spies[0].closed is True, "reset hook did not close the pooled connection -- would leak"
