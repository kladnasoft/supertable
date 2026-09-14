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


def _to_polars(table: pa.Table):
    """Arrow -> polars through the library's own normalisation.

    Using the same normalisation the read path uses is deliberate: the point of
    these tests is that a streamed read equals a buffered one, and a buffered
    read goes through it too. Comparing raw Arrow against a normalised frame
    would fail on type alone and say nothing about the rows.
    """
    from supertable.engine.arrow_result import batches_to_polars

    return batches_to_polars(table.to_batches(), table.schema)


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
    got = _to_polars(_streamed(sql))

    assert list(got.columns) == list(want.columns), sql
    assert len(got) == len(want), f"{len(want)} -> {len(got)}\n{sql}"
    if len(want) == 0:
        return
    cols = list(want.columns)
    # polars compares frames directly, types included — and after the
    # int-sum fix both sides produce the same types, so there is nothing
    # left to paper over with a value-only comparison.
    assert want.sort(cols).equals(got.sort(cols)), (
        f"streamed result differs from buffered\n{sql}"
    )


def test_streaming_preserves_exact_integer_sums():
    """An integer SUM stays an exact integer, in BOTH paths.

    DuckDB types ``SUM`` of a BIGINT as ``decimal128(38, 0)``: exact, 128-bit.
    Neither path may reduce that to float64, which cannot represent consecutive
    integers above 2**53 — so this test constructs a total that float64
    provably cannot hold and requires both paths to return it exactly.

    This used to assert instead that the streamed type was ``decimal`` while
    the buffered one was float, on the grounds that ``execute`` went through
    ``fetchdf()``. It no longer does — buffered assembles Arrow into polars and
    casts a scale-zero decimal to int64, which is exact to 2**63 — so that
    asymmetry was not a property worth sealing: it meant one query answered
    with two different types depending only on how it was called. The stream
    now normalises the same way (``StreamHandle._prepare``), and what is worth
    sealing is the exactness both paths owe, which is what this checks.
    """
    total_sql = "SELECT sum(qty) AS q FROM facts"
    base = int(_buffered(total_sql)["q"].to_list()[0])

    # Offset the real total past 2**53 and make it odd, so float64 cannot
    # represent it: if either path went through float, the value comes back
    # changed. Asserted below rather than assumed.
    offset = 9007199254740993 - (base % 2)
    expected = base + offset
    assert int(float(expected)) != expected, (
        f"{expected} is representable in float64, so this test would not "
        f"detect a float round-trip; adjust the offset"
    )

    sql = f"SELECT sum(qty) + {offset} AS q FROM facts"
    streamed = _streamed(sql)
    buffered = _buffered(sql)

    streamed_value = streamed.column("q").to_pylist()[0]
    buffered_value = buffered["q"].to_list()[0]

    assert int(streamed_value) == expected, (
        f"streamed sum lost exactness: {streamed_value} != {expected}")
    assert int(buffered_value) == expected, (
        f"buffered sum lost exactness: {buffered_value} != {expected}")

    # And the same type, so a caller cannot tell the paths apart by dtype.
    assert not isinstance(streamed_value, float), (
        f"streamed integer sum came back as float: {streamed_value!r}")
    assert not isinstance(buffered_value, float), (
        f"buffered integer sum came back as float: {buffered_value!r}")
    assert type(streamed_value) is type(buffered_value), (
        f"same query, different types: streamed {type(streamed_value).__name__} "
        f"vs buffered {type(buffered_value).__name__}"
    )


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
    # Column count read from the schema, not hardcoded: the fixture table gains
    # columns when the pruning corpus is extended, and a literal here breaks on
    # a change that has nothing to do with monitoring.
    assert seen[0]["result_shape"] == (rows, len(handle.schema)), (
        seen[0]["result_shape"])
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


# --------------------------------------------------------------------------
# Shutdown
# --------------------------------------------------------------------------

def test_cancel_after_close_is_a_noop():
    """The use-after-free that aborted the process.

    A watcher thread could call interrupt() on a cursor the main thread had
    already closed, reaching freed DuckDB C++ state:

        terminate called without an active exception

    Reproduced 2 runs in 6 before the guard; 0 in 12 after. Being a race, the
    count alone proves little — so the mechanism is pinned here directly.
    """
    from supertable.data_reader import DataReader

    handle = DataReader(super_name=D.SUPER, organization=D.ORG,
                        query="SELECT * FROM facts", source="sdk",
                        ).stream(role_name=D.ROLE, batch_rows=1_000)
    next(handle.batches())
    handle.close()
    for _ in range(50):
        handle.cancel()          # must not touch the freed cursor


def test_shutdown_cancels_inflight_jobs():
    from supertable.streaming import JobStore, submit_and_run
    from supertable.streaming.runner import (
        inflight_job_ids, shutdown, _SHUTTING_DOWN,
    )

    store = JobStore()
    rec = submit_and_run(
        D.ORG, D.SUPER,
        "SELECT f.fact_id, e.ev_id FROM facts f, events e",
        D.ROLE, background=True, batch_rows=4_000,
    )
    deadline = time.time() + 30
    while time.time() < deadline and rec.job_id not in inflight_job_ids():
        time.sleep(0.05)
    assert rec.job_id in inflight_job_ids(), "job never registered as in-flight"

    try:
        stopped = shutdown(timeout=10.0)
        assert stopped >= 1
        deadline = time.time() + 20
        while time.time() < deadline:
            if store.get(D.ORG, rec.job_id).state in (
                    "cancelled", "done", "failed", "expired"):
                break
            time.sleep(0.1)
        assert store.get(D.ORG, rec.job_id).state != "running", (
            "shutdown left a job running")
    finally:
        # Other tests in this module still need to start jobs.
        _SHUTTING_DOWN.clear()
        store.delete(D.ORG, rec.job_id)


def test_finished_job_leaves_no_inflight_entry():
    """A registry that only grows would leak a handle per job."""
    from supertable.streaming import JobStore, run_job
    from supertable.streaming.runner import inflight_job_ids

    store = JobStore()
    rec = store.create(D.ORG, D.SUPER, "SELECT * FROM facts", D.ROLE,
                       batch_rows=8_000)
    run_job(rec, store)
    assert rec.job_id not in inflight_job_ids()
    store.delete(D.ORG, rec.job_id)


def test_shutdown_is_safe_with_nothing_running():
    from supertable.streaming.runner import shutdown, _SHUTTING_DOWN

    try:
        assert shutdown(timeout=1.0) == 0
    finally:
        _SHUTTING_DOWN.clear()


# --------------------------------------------------------------------------
# Duplicate column names (audit H3)
# --------------------------------------------------------------------------

def test_join_with_duplicate_column_names_works_both_ways():
    """The same query must not succeed streamed and fail buffered.

    Arrow permits duplicate field names and DuckDB produces them routinely —
    a join on a shared key yields two columns of that name. polars refuses to
    build a frame from that, so execute() raised DuplicateError while stream()
    returned the rows: identical SQL, two outcomes, decided only by how the
    caller asked. A regression from the pandas->polars move.
    """
    from supertable.data_reader import DataReader, engine
    from supertable.engine.arrow_result import materialize

    sql = ("SELECT * FROM facts f JOIN customers c ON f.cust_id = c.cust_id "
           "LIMIT 5")

    buffered, status, msg = DataReader(
        super_name=D.SUPER, organization=D.ORG, query=sql, source="sdk",
    ).execute(role_name=D.ROLE, with_scan=False, engine=engine.AUTO)
    assert str(status).endswith("OK"), msg

    streamed = materialize(DataReader(
        super_name=D.SUPER, organization=D.ORG, query=sql, source="sdk",
    ).stream(role_name=D.ROLE))

    assert len(buffered) == len(streamed) == 5
    assert list(buffered.columns) == list(streamed.columns)


def test_the_duplicate_is_suffixed_not_dropped():
    """Both columns carry real and different data, so neither may be lost.

    The first keeps the bare name so an unambiguous reference still resolves.
    """
    from supertable.data_reader import DataReader, engine

    df, status, msg = DataReader(
        super_name=D.SUPER, organization=D.ORG,
        query="SELECT * FROM facts f JOIN customers c ON f.cust_id = c.cust_id "
              "LIMIT 3",
        source="sdk",
    ).execute(role_name=D.ROLE, with_scan=False, engine=engine.AUTO)

    assert str(status).endswith("OK"), msg
    assert "cust_id" in df.columns and "cust_id_1" in df.columns
    assert len(set(df.columns)) == len(df.columns), "names must be unique"


def test_unique_column_names_are_left_alone():
    """The common case must be untouched — no suffixes, no renaming."""
    from supertable.engine.arrow_result import deduplicate_column_names
    import pyarrow as pa

    table = pa.table({"a": [1], "b": [2]})
    assert deduplicate_column_names(table) is table


def test_a_suffix_that_would_collide_is_skipped():
    """If `id_1` already exists, the duplicate `id` must not overwrite it."""
    from supertable.engine.arrow_result import deduplicate_column_names
    import pyarrow as pa

    table = pa.table({"id": [1], "id_1": [2]}).append_column(
        "id", pa.array([3]))
    out = deduplicate_column_names(table)
    assert len(set(out.schema.names)) == 3
    assert out.schema.names[1] == "id_1", "the pre-existing column keeps its name"
