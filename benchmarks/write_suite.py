"""Write performance suite.

The suite separates two things that are often conflated:

* **Throughput phases** are time-boxed — run for N seconds and report how much
  landed.  How much lands depends on how fast the engine is, so their row
  counts are *not* reproducible and must never be sealed.  What they can prove
  is consistency: the rows the threads believe they wrote must equal the rows
  the table actually returns.  A parallel writer that loses or duplicates rows
  fails here even if it is fast.

* **The lifecycle phase** does a fixed amount of deterministic work —
  append, then delete a fixed subset, then update a fixed subset — and hashes
  the resulting table.  That hash is the seal: a later version doing the same
  fixed work must produce the same content, or its behaviour changed.

Every table created here is dropped afterwards; only the read dataset is kept.
"""
from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pyarrow as pa

from benchmarks import _harness
from benchmarks._harness import (
    BENCH_ORG,
    BENCH_ROLE,
    ScenarioResult,
    expect,
    finish_run,
    new_run,
    run_scenario,
    seal_rows,
    summarize_ms,
)

KEY = "record_id"
LIFECYCLE_ROWS = 20_000
DELETE_EVERY = 7          # delete every 7th key
UPDATE_EVERY = 5          # update every 5th surviving key


# ──────────────────────────────────────────────────────────────────────
# Data generation
# ──────────────────────────────────────────────────────────────────────

def make_batch(start_key: int, count: int, *, seed: int, revision: int = 0) -> pa.Table:
    """A deterministic batch of rows keyed from ``start_key``.

    ``revision`` changes the payload but not the keys, which is what an update
    needs: same identity, different values.
    """
    rng = np.random.default_rng(seed)
    keys = np.arange(start_key, start_key + count, dtype=np.int64)
    return pa.table({
        KEY: pa.array(keys, type=pa.int64()),
        "revision": pa.array(np.full(count, revision, dtype=np.int64), type=pa.int64()),
        "category": pa.array(
            [f"cat_{int(v)}" for v in rng.integers(0, 8, size=count)], type=pa.string()
        ),
        "amount": pa.array(np.round(rng.uniform(1, 5000, size=count), 2),
                           type=pa.float64()),
        "quantity": pa.array(rng.integers(1, 50, size=count, dtype=np.int64),
                             type=pa.int64()),
    })


def key_only_batch(keys: List[int]) -> pa.Table:
    """Minimal frame identifying rows to delete."""
    return pa.table({
        KEY: pa.array(np.array(keys, dtype=np.int64), type=pa.int64()),
        "revision": pa.array(np.zeros(len(keys), dtype=np.int64), type=pa.int64()),
        "category": pa.array(["" for _ in keys], type=pa.string()),
        "amount": pa.array(np.zeros(len(keys), dtype=np.float64), type=pa.float64()),
        "quantity": pa.array(np.zeros(len(keys), dtype=np.int64), type=pa.int64()),
    })


# ──────────────────────────────────────────────────────────────────────
# Table helpers
# ──────────────────────────────────────────────────────────────────────

def _writer():
    from supertable.data_writer import DataWriter
    from supertable.super_table import SuperTable

    SuperTable(super_name=_harness.BENCH_SUPER, organization=BENCH_ORG)
    return DataWriter(super_name=_harness.BENCH_SUPER, organization=BENCH_ORG)


def drop_table(table: str, log=print) -> None:
    try:
        from supertable.simple_table import SimpleTable
        from supertable.super_table import SuperTable

        super_table = SuperTable(super_name=_harness.BENCH_SUPER, organization=BENCH_ORG)
        SimpleTable(super_table=super_table, simple_name=table).delete(
            role_name=BENCH_ROLE
        )
    except Exception:
        pass                                     # absent table is the normal case


def count_rows(table: str) -> Optional[int]:
    from supertable.data_reader import DataReader, engine

    reader = DataReader(
        super_name=_harness.BENCH_SUPER, organization=BENCH_ORG,
        query=f"SELECT COUNT(*) AS n FROM {table}",
    )
    df, status, message = reader.execute(
        role_name=BENCH_ROLE, with_scan=False, engine=engine.AUTO,
    )
    if not str(status).endswith("OK"):
        raise RuntimeError(f"count failed ({status}): {message}")
    rows = df.rows() if hasattr(df, "rows") else list(df.itertuples(index=False, name=None))
    return int(rows[0][0]) if rows else None


def fetch_all(table: str) -> Tuple[List[str], List[Tuple]]:
    from supertable.data_reader import DataReader, engine

    reader = DataReader(
        super_name=_harness.BENCH_SUPER, organization=BENCH_ORG,
        query=f"SELECT * FROM {table}",
    )
    df, status, message = reader.execute(
        role_name=BENCH_ROLE, with_scan=False, engine=engine.AUTO,
    )
    if not str(status).endswith("OK"):
        raise RuntimeError(f"read failed ({status}): {message}")
    if hasattr(df, "rows"):
        return [str(c) for c in df.columns], df.rows()
    return [str(c) for c in df.columns], list(df.itertuples(index=False, name=None))


# ──────────────────────────────────────────────────────────────────────
# Throughput phases
# ──────────────────────────────────────────────────────────────────────

@dataclass
class ThreadOutcome:
    thread: int
    table: str
    batches: int = 0
    rows: int = 0
    errors: int = 0
    first_error: str = ""
    setup_ms: float = 0.0
    latencies_ms: List[float] = field(default_factory=list)


class _StartGate:
    """Hold every writer at the line until all of them are constructed.

    Building a ``DataWriter`` touches storage and the catalog, and doing it
    inside the timed window makes slow setup indistinguishable from slow
    writing: a thread whose construction outlives the deadline reports zero
    rows and zero errors, which reads as "the engine did nothing" when it
    actually means "the clock ran out before this thread was ready".  The
    deadline is therefore started by the last thread to finish setting up.
    """

    def __init__(self, parties: int, duration_s: float) -> None:
        self.deadline = 0.0
        self.duration_s = duration_s
        self.started_at = 0.0
        self.barrier = threading.Barrier(parties, action=self._begin)

    def _begin(self) -> None:
        self.started_at = time.perf_counter()
        self.deadline = self.started_at + self.duration_s

    def wait(self) -> float:
        try:
            self.barrier.wait()
        except threading.BrokenBarrierError:
            # A peer failed to set up.  Measure the remaining threads rather
            # than hanging the whole phase on a party that will never arrive.
            if not self.deadline:
                self._begin()
        return self.deadline

    def abandon(self) -> None:
        """Release peers waiting on a thread that will never reach the line."""
        self.barrier.abort()
        if not self.deadline:
            self._begin()


def _throughput_worker(
    thread_index: int,
    table: str,
    gate: "_StartGate",
    key_base: int,
    batch_bounds: Tuple[int, int],
    outcome: ThreadOutcome,
) -> None:
    """Write batches until the shared deadline, recording per-batch latency."""
    setup_started = time.perf_counter()
    try:
        writer = _writer()                        # one writer per thread
    except BaseException as exc:
        # A thread that dies constructing its writer would otherwise report
        # zero rows and zero errors — indistinguishable from an engine that
        # simply did no work.  Record it, and still release the barrier so the
        # remaining threads are not left waiting for a party that never comes.
        outcome.setup_ms = (time.perf_counter() - setup_started) * 1000.0
        outcome.errors += 1
        outcome.first_error = f"writer setup failed: {type(exc).__name__}: {exc}"
        gate.abandon()
        return
    outcome.setup_ms = (time.perf_counter() - setup_started) * 1000.0
    deadline = gate.wait()                        # clock starts once all are ready
    rng = random.Random(1000 + thread_index)
    next_key = key_base
    while time.perf_counter() < deadline:
        size = rng.randint(*batch_bounds)
        batch = make_batch(next_key, size, seed=next_key)
        started = time.perf_counter()
        try:
            writer.write(
                role_name=BENCH_ROLE, simple_name=table, data=batch,
                overwrite_columns=[],             # append-only: every row lands
            )
            outcome.latencies_ms.append((time.perf_counter() - started) * 1000.0)
            outcome.batches += 1
            outcome.rows += size
        except Exception as exc:
            outcome.errors += 1
            if not outcome.first_error:
                outcome.first_error = f"{type(exc).__name__}: {exc}"
        next_key += size


def parallel_distinct_tables(
    threads: int, duration_s: float, batch_bounds: Tuple[int, int], log=print,
) -> Dict[str, Any]:
    """N threads, each writing to its own table, for a fixed wall-clock window."""
    tables = [f"perf_write_par_{i}" for i in range(threads)]
    for table in tables:
        drop_table(table)

    outcomes = [ThreadOutcome(thread=i, table=tables[i]) for i in range(threads)]
    gate = _StartGate(threads, duration_s)
    workers = [
        threading.Thread(
            target=_throughput_worker,
            args=(i, tables[i], gate, i * 50_000_000, batch_bounds, outcomes[i]),
            name=f"perf-write-{i}",
        )
        for i in range(threads)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    elapsed = time.perf_counter() - gate.started_at

    per_table: Dict[str, Any] = {}
    checks: Dict[str, Any] = {}
    total_written = 0
    total_observed = 0
    for outcome in outcomes:
        observed = None
        try:
            observed = count_rows(outcome.table)
        except Exception as exc:
            checks.update(expect(f"{outcome.table}_readable", False, str(exc), "readable"))
        per_table[outcome.table] = {
            "rows_written": outcome.rows,
            "rows_observed": observed,
            "batches": outcome.batches,
            "errors": outcome.errors,
            "first_error": outcome.first_error,
            # Writer construction, excluded from the timed window but recorded:
            # a thread that takes seconds to reach the start line is telling
            # you something about concurrent setup cost.
            "setup_ms": round(outcome.setup_ms, 1),
            "rows_per_second": round(outcome.rows / elapsed, 1) if elapsed else 0,
            "batch_latency_ms": summarize_ms(outcome.latencies_ms),
        }
        total_written += outcome.rows
        if observed is not None:
            total_observed += observed
        checks.update(expect(
            f"{outcome.table}_rows_match", observed == outcome.rows, observed,
            outcome.rows,
        ))
        checks.update(expect(
            f"{outcome.table}_no_errors", outcome.errors == 0, outcome.errors, 0,
        ))

    for table in tables:
        drop_table(table)

    return {
        "metrics": {
            "threads": threads,
            "duration_s": round(elapsed, 2),
            "batch_rows_min": batch_bounds[0],
            "batch_rows_max": batch_bounds[1],
            "total_rows_written": total_written,
            "total_rows_observed": total_observed,
            "total_rows_per_second": round(total_written / elapsed, 1) if elapsed else 0,
            "setup_ms": summarize_ms([o.setup_ms for o in outcomes]),
            "per_table": per_table,
        },
        "rows": total_written,
        "expectations": checks,
    }


def parallel_same_table(
    threads: int, duration_s: float, batch_bounds: Tuple[int, int], log=print,
) -> Dict[str, Any]:
    """N threads contending on ONE table — the contention case.

    Keys are disjoint per thread and the write is append-only, so the row count
    is an exact accounting check: every row written must be readable, and no
    row may appear twice.
    """
    table = "perf_write_shared"
    drop_table(table)

    outcomes = [ThreadOutcome(thread=i, table=table) for i in range(threads)]
    gate = _StartGate(threads, duration_s)
    workers = [
        threading.Thread(
            target=_throughput_worker,
            args=(i, table, gate, i * 50_000_000, batch_bounds, outcomes[i]),
            name=f"perf-shared-{i}",
        )
        for i in range(threads)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    elapsed = time.perf_counter() - gate.started_at

    written = sum(o.rows for o in outcomes)
    errors = sum(o.errors for o in outcomes)
    observed = count_rows(table)
    distinct = _count_distinct_keys(table)

    checks = {}
    checks.update(expect("rows_match", observed == written, observed, written))
    checks.update(expect("no_duplicate_keys", distinct == observed, distinct, observed))
    checks.update(expect("no_write_errors", errors == 0, errors, 0))

    all_latencies = [ms for o in outcomes for ms in o.latencies_ms]
    metrics = {
        "threads": threads,
        "duration_s": round(elapsed, 2),
        "rows_written": written,
        "rows_observed": observed,
        "distinct_keys": distinct,
        "total_rows_per_second": round(written / elapsed, 1) if elapsed else 0,
        "batch_latency_ms": summarize_ms(all_latencies),
        "setup_ms": summarize_ms([o.setup_ms for o in outcomes]),
        "per_thread": [
            {"thread": o.thread, "batches": o.batches, "rows": o.rows,
             "errors": o.errors, "first_error": o.first_error,
             "setup_ms": round(o.setup_ms, 1)}
            for o in outcomes
        ],
    }
    drop_table(table)
    return {"metrics": metrics, "rows": written, "expectations": checks}


def _count_distinct_keys(table: str) -> Optional[int]:
    from supertable.data_reader import DataReader, engine

    reader = DataReader(
        super_name=_harness.BENCH_SUPER, organization=BENCH_ORG,
        query=f"SELECT COUNT(DISTINCT {KEY}) AS n FROM {table}",
    )
    df, status, _ = reader.execute(
        role_name=BENCH_ROLE, with_scan=False, engine=engine.AUTO,
    )
    if not str(status).endswith("OK"):
        return None
    rows = df.rows() if hasattr(df, "rows") else list(df.itertuples(index=False, name=None))
    return int(rows[0][0]) if rows else None


def serial_append(duration_s: float, batch_rows: int, log=print) -> Dict[str, Any]:
    """Single-threaded append for a fixed window — the uncontended baseline."""
    table = "perf_write_serial"
    drop_table(table)
    writer = _writer()

    deadline = time.perf_counter() + duration_s
    latencies: List[float] = []
    rows = 0
    next_key = 0
    errors = 0
    first_error = ""
    started = time.perf_counter()
    while time.perf_counter() < deadline:
        batch = make_batch(next_key, batch_rows, seed=next_key)
        t0 = time.perf_counter()
        try:
            writer.write(role_name=BENCH_ROLE, simple_name=table, data=batch,
                         overwrite_columns=[])
            latencies.append((time.perf_counter() - t0) * 1000.0)
            rows += batch_rows
        except Exception as exc:
            errors += 1
            if not first_error:
                first_error = f"{type(exc).__name__}: {exc}"
        next_key += batch_rows
    elapsed = time.perf_counter() - started

    observed = count_rows(table)
    checks = {}
    checks.update(expect("rows_match", observed == rows, observed, rows))
    checks.update(expect("no_write_errors", errors == 0, errors, 0))
    metrics = {
        "duration_s": round(elapsed, 2),
        "batch_rows": batch_rows,
        "batches": len(latencies),
        "rows_written": rows,
        "rows_observed": observed,
        "rows_per_second": round(rows / elapsed, 1) if elapsed else 0,
        "write_latency_ms": summarize_ms(latencies),
        "errors": errors,
        "first_error": first_error,
    }
    drop_table(table)
    return {"metrics": metrics, "rows": rows, "expectations": checks}


# ──────────────────────────────────────────────────────────────────────
# Deterministic lifecycle — this is what gets sealed
# ──────────────────────────────────────────────────────────────────────

def lifecycle(log=print) -> Dict[str, Any]:
    """Fixed work: append, delete a fixed subset, update a fixed subset, hash.

    Because the work is fixed rather than time-boxed, the resulting table is
    reproducible, so its hash is a genuine cross-version seal: the same three
    operations must yield the same content on any version that behaves the
    same way.
    """
    table = "perf_write_lifecycle"
    drop_table(table)
    writer = _writer()

    timings: Dict[str, float] = {}

    # --- append ------------------------------------------------------------
    batch = make_batch(0, LIFECYCLE_ROWS, seed=99, revision=0)
    t0 = time.perf_counter()
    writer.write(role_name=BENCH_ROLE, simple_name=table, data=batch,
                 overwrite_columns=[])
    timings["append_ms"] = round((time.perf_counter() - t0) * 1000.0, 3)
    after_append = count_rows(table)

    # --- delete a reproducible subset --------------------------------------
    delete_keys = list(range(0, LIFECYCLE_ROWS, DELETE_EVERY))
    t0 = time.perf_counter()
    result = writer.write(
        role_name=BENCH_ROLE, simple_name=table, data=key_only_batch(delete_keys),
        overwrite_columns=[KEY], delete_only=True,
    )
    timings["delete_ms"] = round((time.perf_counter() - t0) * 1000.0, 3)
    deleted_reported = result[3] if isinstance(result, (tuple, list)) and len(result) > 3 else None
    after_delete = count_rows(table)

    # --- update a reproducible subset of the survivors ----------------------
    survivors = [k for k in range(LIFECYCLE_ROWS) if k % DELETE_EVERY != 0]
    update_keys = survivors[::UPDATE_EVERY]
    update_batch = make_batch(0, LIFECYCLE_ROWS, seed=99, revision=1)
    mask = np.isin(update_batch.column(KEY).to_numpy(), np.array(update_keys))
    update_slice = update_batch.filter(pa.array(mask))
    t0 = time.perf_counter()
    writer.write(role_name=BENCH_ROLE, simple_name=table, data=update_slice,
                 overwrite_columns=[KEY])
    timings["update_ms"] = round((time.perf_counter() - t0) * 1000.0, 3)
    after_update = count_rows(table)

    # --- seal ---------------------------------------------------------------
    columns, rows = fetch_all(table)
    seal = seal_rows(columns, rows)

    expected_after_delete = LIFECYCLE_ROWS - len(delete_keys)
    checks = {}
    checks.update(expect("rows_after_append", after_append == LIFECYCLE_ROWS,
                         after_append, LIFECYCLE_ROWS))
    checks.update(expect("rows_after_delete", after_delete == expected_after_delete,
                         after_delete, expected_after_delete))
    checks.update(expect("update_preserves_row_count",
                         after_update == expected_after_delete,
                         after_update, expected_after_delete))
    checks.update(expect("deleted_rows_gone",
                         _keys_absent(table, delete_keys[:50]), True, True))
    revised = _count_revision(table, 1)
    checks.update(expect("updated_rows_carry_new_revision",
                         revised == len(update_keys), revised, len(update_keys)))

    metrics = {
        "seed_rows": LIFECYCLE_ROWS,
        "deleted_keys": len(delete_keys),
        "deleted_reported": deleted_reported,
        "updated_keys": len(update_keys),
        "rows_after_append": after_append,
        "rows_after_delete": after_delete,
        "rows_after_update": after_update,
        "timings": timings,
    }
    drop_table(table)
    return {"metrics": metrics, "rows": after_update, "seal": seal,
            "expectations": checks}


def _keys_absent(table: str, keys: List[int]) -> bool:
    from supertable.data_reader import DataReader, engine

    key_list = ",".join(str(k) for k in keys)
    reader = DataReader(
        super_name=_harness.BENCH_SUPER, organization=BENCH_ORG,
        query=f"SELECT COUNT(*) AS n FROM {table} WHERE {KEY} IN ({key_list})",
    )
    df, status, _ = reader.execute(
        role_name=BENCH_ROLE, with_scan=False, engine=engine.AUTO,
    )
    if not str(status).endswith("OK"):
        return False
    rows = df.rows() if hasattr(df, "rows") else list(df.itertuples(index=False, name=None))
    return bool(rows) and int(rows[0][0]) == 0


def _count_revision(table: str, revision: int) -> Optional[int]:
    from supertable.data_reader import DataReader, engine

    reader = DataReader(
        super_name=_harness.BENCH_SUPER, organization=BENCH_ORG,
        query=f"SELECT COUNT(*) AS n FROM {table} WHERE revision = {revision}",
    )
    df, status, _ = reader.execute(
        role_name=BENCH_ROLE, with_scan=False, engine=engine.AUTO,
    )
    if not str(status).endswith("OK"):
        return None
    rows = df.rows() if hasattr(df, "rows") else list(df.itertuples(index=False, name=None))
    return int(rows[0][0]) if rows else None


# ──────────────────────────────────────────────────────────────────────
# Additional shapes
# ──────────────────────────────────────────────────────────────────────

def small_write_latency(count: int, log=print) -> Dict[str, Any]:
    """Many tiny writes — isolates per-write fixed cost from per-row cost.

    Overwrite latency is dominated by small-object PUT cost and engine warmup
    rather than row count, so this is the scenario a per-write regression shows
    up in first.
    """
    table = "perf_write_small"
    drop_table(table)
    writer = _writer()
    latencies: List[float] = []
    for i in range(count):
        t0 = time.perf_counter()
        writer.write(role_name=BENCH_ROLE, simple_name=table,
                     data=make_batch(i, 1, seed=i), overwrite_columns=[])
        latencies.append((time.perf_counter() - t0) * 1000.0)
    observed = count_rows(table)
    checks = expect("rows_match", observed == count, observed, count)
    drop_table(table)
    return {
        "metrics": {"writes": count, "rows_observed": observed,
                    "write_latency_ms": summarize_ms(latencies)},
        "rows": count,
        "expectations": checks,
    }


def large_batch_write(rows: int, log=print) -> Dict[str, Any]:
    """One large append — the per-row cost case, with fixed cost amortised."""
    table = "perf_write_large"
    drop_table(table)
    writer = _writer()
    batch = make_batch(0, rows, seed=4242)
    t0 = time.perf_counter()
    writer.write(role_name=BENCH_ROLE, simple_name=table, data=batch,
                 overwrite_columns=[])
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    observed = count_rows(table)
    checks = expect("rows_match", observed == rows, observed, rows)
    drop_table(table)
    return {
        "metrics": {"rows": rows, "elapsed_ms": round(elapsed_ms, 3),
                    "rows_per_second": round(rows / (elapsed_ms / 1000.0), 1)},
        "rows": rows,
        "expectations": checks,
    }


def upsert_existing_keys(rows: int, log=print) -> Dict[str, Any]:
    """Rewrite every key that already exists — the merge-on-read path.

    Row count must not grow: an upsert that appends instead of replacing is a
    correctness failure this catches regardless of timing.
    """
    table = "perf_write_upsert"
    drop_table(table)
    writer = _writer()

    writer.write(role_name=BENCH_ROLE, simple_name=table,
                 data=make_batch(0, rows, seed=11, revision=0),
                 overwrite_columns=[KEY])
    baseline = count_rows(table)

    t0 = time.perf_counter()
    writer.write(role_name=BENCH_ROLE, simple_name=table,
                 data=make_batch(0, rows, seed=11, revision=1),
                 overwrite_columns=[KEY])
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    after = count_rows(table)
    revised = _count_revision(table, 1)

    checks = {}
    checks.update(expect("row_count_stable", after == rows, after, rows))
    checks.update(expect("all_rows_revised", revised == rows, revised, rows))
    drop_table(table)
    return {
        "metrics": {"rows": rows, "rows_before": baseline, "rows_after": after,
                    "upsert_ms": round(elapsed_ms, 3)},
        "rows": after,
        "expectations": checks,
    }


# ──────────────────────────────────────────────────────────────────────
# Suite
# ──────────────────────────────────────────────────────────────────────

def scenarios(duration_s: float, scale_name: str) -> List[Dict[str, Any]]:
    small = scale_name != "full"
    return [
        {
            "id": "parallel_8_distinct_tables",
            "description": f"8 threads writing 1000-10000 row batches to their "
                           f"OWN tables for {duration_s:g}s; per-table and total ingest",
            "body": lambda: parallel_distinct_tables(8, duration_s, (1000, 10000)),
        },
        {
            "id": "parallel_4_same_table",
            "description": f"4 threads contending on ONE table for {duration_s:g}s; "
                           f"row count and key uniqueness verified",
            "body": lambda: parallel_same_table(4, duration_s, (1000, 10000)),
        },
        {
            "id": "serial_append_only",
            "description": f"single-threaded append for {duration_s:g}s — "
                           f"uncontended baseline",
            "body": lambda: serial_append(duration_s, 5000),
        },
        {
            "id": "lifecycle_append_delete_update_sealed",
            "description": "fixed deterministic append + delete + update, then "
                           "hash the table — the cross-version correctness seal",
            "body": lifecycle,
        },
        {
            "id": "small_write_latency",
            "description": "many single-row writes — per-write fixed cost, "
                           "where PUT and warmup overhead dominate",
            "body": lambda: small_write_latency(20 if small else 100),
        },
        {
            "id": "large_batch_write",
            "description": "one large append — per-row cost with fixed cost "
                           "amortised away",
            "body": lambda: large_batch_write(50_000 if small else 500_000),
        },
        {
            "id": "upsert_existing_keys",
            "description": "rewrite every existing key (merge-on-read); row "
                           "count must stay flat",
            "body": lambda: upsert_existing_keys(10_000 if small else 100_000),
        },
    ]


def run(profile: str, scale_name: str, *, duration_s: float = 60.0, log=print):
    started = time.perf_counter()
    suite = new_run("write", profile, scale_name)

    log(f"\nwrite suite — profile={profile} scale={scale_name} "
        f"phase_duration={duration_s:g}s")
    for spec in scenarios(duration_s, scale_name):
        result: ScenarioResult = run_scenario(
            spec["id"], spec["description"], spec["body"],
            iterations=1, warmup=0, log=log,
        )
        suite.scenarios.append(result)

    return finish_run(suite, started)
