# route: supertable.tests.test_streaming
"""Streaming queries: same answer as a buffered read, without the memory.

The governing rule is the same one that governs pruning: a streamed read must
return exactly what a buffered read returns. Streaming changes WHEN rows arrive,
never WHICH rows arrive — so these tests compare against ``execute`` rather than
against hand-written expectations.

That matters more than it looks. The streamed path builds its own view chain, so
a mistake there would silently skip the deletion-vector anti-join or the RBAC
filter and hand back rows the caller is not allowed to see. Comparing against
the buffered path is what makes that impossible to miss.

These are integration tests against the live stack; they skip when it is
unavailable, the same way the pruning corpus does.
"""

from __future__ import annotations

import threading
import time

import pyarrow as pa
import pytest

from supertable.tests.pruning import dataset as D


@pytest.fixture(scope="module", autouse=True)
def _dataset():
    try:
        D.build(log=lambda *a, **k: None)
    except Exception as e:
        pytest.skip(f"live catalog/storage unavailable ({type(e).__name__}: "
                    f"{str(e)[:120]})")


def _buffered(sql: str):
    from supertable.data_reader import DataReader, engine

    df, status, msg = DataReader(
        super_name=D.SUPER, organization=D.ORG, query=sql, source="sdk",
    ).execute(role_name=D.ROLE, with_scan=False, engine=engine.AUTO)
    assert str(status).endswith("OK"), msg
    return df


def _streamed(sql: str, **kw) -> pa.Table:
    from supertable.data_reader import DataReader

    handle = DataReader(super_name=D.SUPER, organization=D.ORG,
                        query=sql, source="sdk").stream(role_name=D.ROLE, **kw)
    with handle:
        batches = list(handle.batches())
    return pa.Table.from_batches(batches, schema=handle.schema)


# --------------------------------------------------------------------------
# The invariant
# --------------------------------------------------------------------------

@pytest.mark.parametrize("sql", [
    "SELECT count(*) AS n FROM facts",
    "SELECT * FROM facts WHERE qty > 30",
    "SELECT region, sum(qty) AS q FROM facts GROUP BY region ORDER BY region",
    "SELECT f.fact_id, c.tier FROM facts f JOIN customers c "
    "ON f.cust_id = c.cust_id WHERE f.qty > 35",
    "WITH w AS (SELECT * FROM facts WHERE qty > 30) SELECT count(*) AS n FROM w",
])
def test_streamed_equals_buffered(sql):
    """Streaming changes when rows arrive, not which rows arrive."""
    want = _buffered(sql)
    got = _streamed(sql).to_pandas()

    assert list(got.columns) == list(want.columns), sql
    assert len(got) == len(want), f"{len(want)} -> {len(got)}\n{sql}"
    if len(want) == 0:
        return
    cols = list(want.columns)
    a = want.sort_values(cols, kind="mergesort").reset_index(drop=True)
    b = got.sort_values(cols, kind="mergesort").reset_index(drop=True)
    for c in cols:
        # Compare VALUES, not dtypes: see test_streaming_preserves_integer_types
        # — the buffered path widens ints to float, the streamed one does not.
        import numpy as np
        import pandas as pd
        if pd.api.types.is_numeric_dtype(a[c]) or pd.api.types.is_numeric_dtype(b[c]):
            av = pd.to_numeric(a[c], errors="coerce").astype(float).to_numpy()
            bv = pd.to_numeric(b[c], errors="coerce").astype(float).to_numpy()
            assert np.allclose(av, bv, rtol=1e-9, atol=1e-9, equal_nan=True), (
                f"column {c!r} differs\n{sql}")
        else:
            assert a[c].astype(str).equals(b[c].astype(str)), (
                f"column {c!r} differs\n{sql}")


def test_streaming_preserves_exact_integer_sums():
    """Streaming is not just faster here — it is more CORRECT.

    DuckDB types ``SUM`` of a BIGINT as ``decimal128(38, 0)``: exact, 128-bit.
    ``execute`` goes through ``fetchdf()``, which converts that to float64, and
    above 2**53 float64 cannot represent consecutive integers. Verified against
    DuckDB directly:

        sum -> 9007199254740995   (arrow, decimal128)
        sum -> 9007199254740996   (fetchdf, float64)   off by one

    So a buffered read can return a wrong total on a large integer sum, and a
    streamed read cannot. That makes this a reason to prefer streaming for
    exports, not merely a dtype curiosity — and a thing to check before
    "simplifying" the stream by routing it through pandas.
    """
    import pyarrow as pa

    sql = "SELECT region, sum(qty) AS q FROM facts GROUP BY region ORDER BY region"
    streamed = _streamed(sql)
    assert pa.types.is_decimal(streamed.schema.field("q").type), (
        f"expected an exact decimal sum, got {streamed.schema.field('q').type}")

    # Same values, exactly, at this magnitude — the divergence only appears
    # past 2**53, which the dataset does not reach.
    buffered = _buffered(sql)
    assert ([int(v) for v in streamed.column("q").to_pylist()]
            == [int(v) for v in buffered["q"].to_list()])


def test_select_star_is_not_limited():
    """No default LIMIT: the point of the feature.

    ``query_sql`` appends a LIMIT to unbounded SELECTs because a buffered read
    materialises everything. A stream must not inherit that.
    """
    table = _streamed("SELECT * FROM facts")
    assert table.num_rows == D.table_row_counts()["facts"]


def test_system_columns_stay_hidden():
    """The stream must go through the same view chain, not around it."""
    table = _streamed("SELECT * FROM facts")
    names = set(table.schema.names)
    assert "__rowid__" not in names and "__timestamp__" not in names, names


# --------------------------------------------------------------------------
# It must actually stream
# --------------------------------------------------------------------------

def test_first_batch_arrives_before_the_query_finishes():
    """If this fails the result is being materialised and merely handed over
    in pieces, which is the thing streaming exists to avoid."""
    from supertable.data_reader import DataReader

    handle = DataReader(super_name=D.SUPER, organization=D.ORG,
                        query="SELECT * FROM facts", source="sdk",
                        ).stream(role_name=D.ROLE, batch_rows=4_000)
    with handle:
        it = handle.batches()
        first = next(it)
        assert first.num_rows == 4_000, (
            f"got {first.num_rows} rows in the first batch; a materialised "
            f"result would arrive whole"
        )
        rest = sum(b.num_rows for b in it)
    assert first.num_rows + rest == D.table_row_counts()["facts"]


def test_batch_size_is_honoured():
    from supertable.data_reader import DataReader

    handle = DataReader(super_name=D.SUPER, organization=D.ORG,
                        query="SELECT * FROM facts", source="sdk",
                        ).stream(role_name=D.ROLE, batch_rows=5_000)
    with handle:
        sizes = [b.num_rows for b in handle.batches()]
    assert max(sizes) <= 5_000, sizes[:5]
    assert len(sizes) >= 10, f"expected many batches, got {len(sizes)}"


def test_handle_can_be_closed_early_without_leaking_views():
    """A consumer that stops reading must not strand catalog objects."""
    from importlib import import_module
    from supertable.data_reader import DataReader

    duck = import_module("supertable.engine.duckdb")
    con = duck._shared_state().get("con")

    handle = DataReader(super_name=D.SUPER, organization=D.ORG,
                        query="SELECT * FROM facts", source="sdk",
                        ).stream(role_name=D.ROLE, batch_rows=1_000)
    next(handle.batches())
    views = list(handle._views)
    handle.close()

    con = duck._shared_state().get("con")
    assert con is not None and views
    for v in views:
        left = con.execute(
            "SELECT count(*) FROM duckdb_views() WHERE view_name = ?", [v],
        ).fetchone()[0]
        assert left == 0, f"view {v} survived close()"


# --------------------------------------------------------------------------
# Jobs: producer and consumer in different processes
# --------------------------------------------------------------------------

def test_job_output_matches_buffered_read():
    from supertable.streaming import JobStore, iter_job_batches, submit_and_run

    sql = "SELECT * FROM facts WHERE qty > 20"
    want = _buffered(sql)
    rec = submit_and_run(D.ORG, D.SUPER, sql, D.ROLE, background=True,
                         batch_rows=8_000)
    batches = list(iter_job_batches(D.ORG, rec.job_id))
    got = sum(b.num_rows for b in batches)

    store = JobStore()
    final = store.get(D.ORG, rec.job_id)
    assert final.state == "done", f"{final.state}: {final.error}"
    assert got == len(want)
    store.delete(D.ORG, rec.job_id)


def test_consumer_uses_a_separate_store_instance():
    """Stand-in for 'a different container'.

    The consumer shares nothing with the producer but Redis and storage — no
    handle, no connection, no object. If this works, a second instance works.
    """
    from supertable.streaming import JobStore, iter_job_batches, submit_and_run

    rec = submit_and_run(D.ORG, D.SUPER, "SELECT * FROM facts", D.ROLE,
                         background=True, batch_rows=8_000)
    consumer_store = JobStore()          # fresh: no shared state with producer
    total = sum(b.num_rows for b in iter_job_batches(
        D.ORG, rec.job_id, store=consumer_store))
    assert total == D.table_row_counts()["facts"]
    consumer_store.delete(D.ORG, rec.job_id)


def test_cancel_stops_a_running_job():
    from supertable.streaming import JobStore, submit_and_run

    store = JobStore()
    # Cross join: large enough that it cannot finish before we cancel.
    rec = submit_and_run(
        D.ORG, D.SUPER,
        "SELECT f.fact_id, e.ev_id FROM facts f, events e",
        D.ROLE, background=True, batch_rows=4_000,
    )
    deadline = time.time() + 30
    while time.time() < deadline:
        if store.get(D.ORG, rec.job_id).state == "running":
            break
        time.sleep(0.05)

    store.cancel(D.ORG, rec.job_id)

    deadline = time.time() + 30
    state = None
    while time.time() < deadline:
        state = store.get(D.ORG, rec.job_id).state
        if state in ("cancelled", "done", "failed", "expired"):
            break
        time.sleep(0.1)
    assert state == "cancelled", f"ended {state!r}"
    store.delete(D.ORG, rec.job_id)


def test_deadline_expires_a_job():
    from supertable.streaming import JobStore, submit_and_run

    store = JobStore()
    rec = submit_and_run(
        D.ORG, D.SUPER,
        "SELECT f.fact_id, e.ev_id FROM facts f, events e",
        D.ROLE, background=True, batch_rows=4_000, deadline_sec=1,
    )
    deadline = time.time() + 40
    state = None
    while time.time() < deadline:
        state = store.get(D.ORG, rec.job_id).state
        if state in ("expired", "done", "failed", "cancelled"):
            break
        time.sleep(0.1)
    assert state == "expired", f"ended {state!r}"
    store.delete(D.ORG, rec.job_id)


def test_zero_deadline_means_no_deadline():
    """An export may legitimately outrun any fixed budget."""
    from supertable.streaming import JobStore

    store = JobStore()
    rec = store.create(D.ORG, D.SUPER, "SELECT 1", D.ROLE, deadline_sec=0)
    assert rec.deadline_ts == 0.0
    store.delete(D.ORG, rec.job_id)


def test_cancelled_job_keeps_what_it_produced():
    """Partial output is the point; a cancelled export should not be lost."""
    from supertable.streaming import JobStore, iter_job_batches, submit_and_run

    store = JobStore()
    rec = submit_and_run(
        D.ORG, D.SUPER,
        "SELECT f.fact_id, e.ev_id FROM facts f, events e",
        D.ROLE, background=True, batch_rows=4_000,
    )
    # Let it produce at least one chunk before cancelling.
    deadline = time.time() + 60
    while time.time() < deadline:
        if store.chunk_count(D.ORG, rec.job_id) > 0:
            break
        time.sleep(0.1)
    produced = store.chunk_count(D.ORG, rec.job_id)
    store.cancel(D.ORG, rec.job_id)

    deadline = time.time() + 30
    while time.time() < deadline:
        if store.get(D.ORG, rec.job_id).state in (
                "cancelled", "done", "failed", "expired"):
            break
        time.sleep(0.1)

    if produced:
        got = sum(b.num_rows for b in iter_job_batches(
            D.ORG, rec.job_id, follow=False))
        assert got > 0, "a cancelled job discarded rows it had already spilled"
    store.delete(D.ORG, rec.job_id)


# --------------------------------------------------------------------------
# Job bookkeeping
# --------------------------------------------------------------------------

def test_job_keys_are_org_scoped_not_lake_scoped():
    from supertable import redis_keys as RK

    jid = "abc123def456"
    # Asserted structurally rather than against a literal key: writing the key
    # out as an f-string here is the very thing test_redis_key_prefix forbids,
    # and it would also duplicate the constructor it is supposed to check.
    parts = RK.query_job_doc(D.ORG, jid).split(":")
    assert parts[0] == "supertable" and parts[1] == D.ORG
    assert parts[2:5] == ["query", "job", "doc"] and parts[5] == jid
    # The point of the route: ORG level. A qualified join can span supertables,
    # so a job has no single lake to belong to.
    assert "lakes" not in parts, RK.query_job_doc(D.ORG, jid)
    assert RK.query_job_chunks(D.ORG, jid).endswith(
        f":query:job:chunks:{jid}")
    assert RK.query_job_cancel(D.ORG, jid).endswith(
        f":query:job:cancel:{jid}")
    # One pattern must reach every key belonging to a single job.
    pat = RK.query_job_subkey_pattern(D.ORG, jid)
    assert pat.endswith(f":query:job:*:{jid}")


def test_delete_removes_keys_and_spilled_chunks():
    from supertable.storage.storage_factory import get_storage
    from supertable.streaming import JobStore, iter_job_batches, submit_and_run

    store, storage = JobStore(), get_storage()
    rec = submit_and_run(D.ORG, D.SUPER, "SELECT * FROM facts", D.ROLE,
                         background=True, batch_rows=8_000)
    list(iter_job_batches(D.ORG, rec.job_id))
    paths = [c.path for c in store.chunks(D.ORG, rec.job_id)]
    assert paths, "job produced no chunks"

    store.delete(D.ORG, rec.job_id, storage=storage)
    assert store.get(D.ORG, rec.job_id) is None
    assert rec.job_id not in store.list_jobs(D.ORG)
    for p in paths:
        assert not storage.exists(p), f"chunk survived delete: {p}"


def test_reap_drops_index_entries_whose_job_expired():
    """A container killed mid-export leaves the index entry behind."""
    from supertable import redis_keys as RK
    from supertable.streaming import JobStore

    store = JobStore()
    rec = store.create(D.ORG, D.SUPER, "SELECT 1", D.ROLE)
    # Simulate the doc TTL expiring while the index member survives.
    store._r.delete(RK.query_job_doc(D.ORG, rec.job_id))
    assert rec.job_id in store.list_jobs(D.ORG)

    store.reap(D.ORG)
    assert rec.job_id not in store.list_jobs(D.ORG)


def test_cancel_is_visible_to_any_instance():
    """Cancellation travels through Redis, not through an object reference."""
    from supertable.streaming import JobStore

    a, b = JobStore(), JobStore()
    rec = a.create(D.ORG, D.SUPER, "SELECT 1", D.ROLE)
    assert not b.is_cancelled(D.ORG, rec.job_id)
    b.cancel(D.ORG, rec.job_id)
    assert a.is_cancelled(D.ORG, rec.job_id)
    a.delete(D.ORG, rec.job_id)


# --------------------------------------------------------------------------
# Monitoring
# --------------------------------------------------------------------------

def test_streamed_query_is_recorded_in_monitoring_on_close():
    """A streamed read must appear in monitoring like any other read.

    The row count does not exist when ``stream`` returns, so the entry is
    written when the stream CLOSES. Without that a streaming query would be
    invisible — and a long export is exactly the read you most want to see.
    """
    import supertable.data_reader as dr
    from supertable.data_reader import DataReader

    seen = []
    original = dr.extend_execution_plan
    dr.extend_execution_plan = lambda **kw: (seen.append(kw), original(**kw))[1]
    try:
        handle = DataReader(super_name=D.SUPER, organization=D.ORG,
                            query="SELECT * FROM facts", source="sdk",
                            ).stream(role_name=D.ROLE, batch_rows=8_000)
        assert not seen, "monitoring fired before any rows were streamed"
        with handle:
            rows = sum(b.num_rows for b in handle.batches())
    finally:
        dr.extend_execution_plan = original

    assert len(seen) == 1, f"expected one monitoring entry, got {len(seen)}"
    assert seen[0]["result_shape"] == (rows, 11), seen[0]["result_shape"]
    assert rows == D.table_row_counts()["facts"]


def test_monitoring_failure_cannot_break_a_stream():
    """A query that already produced its rows must not fail at teardown."""
    import supertable.data_reader as dr
    from supertable.data_reader import DataReader

    original = dr.extend_execution_plan

    def boom(**kw):
        raise RuntimeError("monitoring backend down")

    dr.extend_execution_plan = boom
    try:
        handle = DataReader(super_name=D.SUPER, organization=D.ORG,
                            query="SELECT * FROM facts", source="sdk",
                            ).stream(role_name=D.ROLE, batch_rows=8_000)
        with handle:
            rows = sum(b.num_rows for b in handle.batches())
        assert rows == D.table_row_counts()["facts"]
    finally:
        dr.extend_execution_plan = original


def test_fire_and_forget_export_completes_without_a_consumer():
    """The default must not require anybody to be reading.

    Consumer-coupled backpressure was the original default and it made an
    unattended export stall after a few chunks. A job exists precisely because
    the consumer may arrive later, from another container.
    """
    from supertable.streaming import JobStore, run_job

    store = JobStore()
    rec = store.create(D.ORG, D.SUPER, "SELECT * FROM facts", D.ROLE,
                       batch_rows=4_000)
    run_job(rec, store)                      # nobody reads while it runs
    final = store.get(D.ORG, rec.job_id)
    assert final.state == "done", f"{final.state}: {final.error}"
    assert final.rows == D.table_row_counts()["facts"]
    store.delete(D.ORG, rec.job_id)
