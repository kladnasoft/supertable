"""Parallel-write contention seals: siblings and racing creators must not fail.

Two independent defects made concurrent writers inside one SuperTable fail
even though the per-table lease and the snapshot CAS were working correctly.
Both were false denials -- the writers were fully authorized and their bases
were current -- and both surfaced as exceptions the caller could not retry.

1. Sibling-table denial.  ``commit_snapshot`` bumps the shared per-SuperTable
   root document on every commit, and the write-authority fence compared that
   document's ``version``/``ts``.  So any writer whose storage I/O overlapped
   an *unrelated* table's commit was rejected with ``PermissionError``.  Worse,
   ``PermissionError`` is classified as a definite rejection
   (data_writer.py), so the rejected writer also deleted the durable
   objects it had just written.  Measured at 7.3% of writes across two tables;
   the rate scales with resource count because the vulnerable window is
   ``validate -> serialize payload -> EVALSHA``.

2. Racing-creator denial.  The same script rejected publication whenever *any*
   namespace-lock holder existed, unless the caller was a flagged one-shot
   creator.  The namespace lock is taken by table *creation*, not just
   deletion, so racing first-writers wounded each other: 6 of 8 lost, and
   creating a table also broke ordinary appends to unrelated tables.  Both were
   reported as ``RuntimeError("SuperTable namespace is fenced for deletion")``
   while nothing was being deleted.

The genuine fences are unaffected and are sealed elsewhere: RBAC generations
still fence publication (``test_redis_catalog_atomic_updates``), and namespace
deletion is fenced by its durable intent, which ``begin_namespace_deletion``
persists *before* draining any leaf lease (``test_deletion_intent_fencing``).
"""

from __future__ import annotations

import json
import threading

import polars as pl
import pyarrow as pa

from supertable.data_reader import DataReader
from supertable.data_writer import DataWriter
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "demo_parallel"
ROLE = "superadmin"
KEY = "rid"

THREADS = 4
ROWS_PER_WRITE = 25


def _rows(start: int) -> pa.Table:
    return pa.table({
        KEY: list(range(start, start + ROWS_PER_WRITE)),
        "amount": [float(i) for i in range(ROWS_PER_WRITE)],
    })


def _run_parallel(targets: list[tuple[str, int]]) -> list[BaseException]:
    """Write each (table, id_offset) concurrently; return raised exceptions."""
    errors: list[BaseException] = []
    guard = threading.Lock()
    start = threading.Barrier(len(targets))

    def worker(simple_name: str, offset: int) -> None:
        writer = DataWriter(super_name=SUPER, organization=ORG)
        start.wait(timeout=30)
        try:
            writer.write(
                role_name=ROLE,
                simple_name=simple_name,
                data=_rows(offset),
                overwrite_columns=[],
            )
        except BaseException as exc:  # noqa: BLE001 - recorded, then asserted
            with guard:
                errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=target) for target in targets
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)
    assert not any(t.is_alive() for t in threads), "a writer thread hung"
    return errors


def _row_count(simple_name: str) -> int:
    reader = DataReader(
        super_name=SUPER,
        organization=ORG,
        query=f"SELECT count(*) AS n FROM {simple_name}",
    )
    out, status, message = reader.execute(role_name=ROLE, with_scan=False)
    assert str(status).endswith("OK"), f"read failed: {status} / {message}"
    frame = out if isinstance(out, pl.DataFrame) else pl.from_pandas(out)
    return int(frame["n"][0])


def test_sibling_table_commit_inside_the_window_does_not_deny(monkeypatch):
    """Deterministic seal: a sibling commit lands in the vulnerable window.

    Racing real threads reproduces this only probabilistically -- the window is
    ``validate -> serialize payload -> EVALSHA``, which is sub-millisecond
    against in-process fakeredis (it took 96 writes on real Redis to observe
    7.3%).  Instead, advance the shared root immediately before every
    delegation to ``commit_snapshot``, which is exactly what a sibling table's
    commit does to shared state, and require the writer to still publish.
    """
    SuperTable(SUPER, ORG)
    writer = DataWriter(super_name=SUPER, organization=ORG)
    writer.write(
        role_name=ROLE, simple_name="window_t", data=_rows(0),
        overwrite_columns=[],
    )

    from supertable import redis_keys as RK
    from supertable.redis_catalog import RedisCatalog

    real_commit = RedisCatalog.commit_snapshot

    def commit_after_sibling_bump(self, org, sup, simple, *args, **kwargs):
        root_key = RK.meta_root(org, sup)
        root = json.loads(self.r.get(root_key))
        root["version"] = int(root["version"]) + 1
        root["ts"] = int(root["ts"]) + 1
        self.r.set(root_key, json.dumps(root))
        return real_commit(self, org, sup, simple, *args, **kwargs)

    monkeypatch.setattr(
        RedisCatalog, "commit_snapshot", commit_after_sibling_bump,
    )

    writer.write(
        role_name=ROLE, simple_name="window_t", data=_rows(500),
        overwrite_columns=[],
    )
    assert _row_count("window_t") == ROWS_PER_WRITE * 2


def test_a_table_named_roles_init_is_writable():
    """The RBAC bootstrap lock must not squat on the per-table key space.

    ``RoleManager._init_role_storage`` used to take its bootstrap lease via
    ``acquire_simple_lock(org, sup, "roles_init")``, i.e. the *same* Redis key
    a table named ``roles_init`` locks.  Table names are not a reserved
    namespace, so such a table self-deadlocked against its own RBAC bootstrap
    (30s spin -> TimeoutError), and the namespace-deletion drain -- which
    recovers table names from ``lock:leaf:doc:`` keys -- saw the bootstrap
    lease as a table.
    """
    SuperTable(SUPER, ORG)
    writer = DataWriter(super_name=SUPER, organization=ORG)
    writer.write(
        role_name=ROLE, simple_name="roles_init", data=_rows(0),
        overwrite_columns=[],
    )
    assert _row_count("roles_init") == ROWS_PER_WRITE


def test_sibling_table_writes_do_not_deny_each_other():
    """Concurrent writers to different tables must all commit."""
    SuperTable(SUPER, ORG)
    writer = DataWriter(super_name=SUPER, organization=ORG)
    # Establish both tables first so this exercises the steady-state append
    # path rather than the creation path covered by the next test.
    for simple_name in ("sib_a", "sib_b"):
        writer.write(
            role_name=ROLE, simple_name=simple_name, data=_rows(0),
            overwrite_columns=[],
        )

    targets = [
        ("sib_a" if index % 2 == 0 else "sib_b", 1000 + index * ROWS_PER_WRITE)
        for index in range(THREADS)
    ]
    errors = _run_parallel(targets)

    assert errors == [], (
        "a sibling table's commit denied an unrelated authorized writer: "
        + "; ".join(f"{type(e).__name__}: {e}" for e in errors)
    )
    # Every write must be durable, not merely un-raised.
    for simple_name in ("sib_a", "sib_b"):
        expected = ROWS_PER_WRITE * (
            1 + sum(1 for name, _ in targets if name == simple_name)
        )
        assert _row_count(simple_name) == expected


def test_racing_first_writes_to_a_new_table_all_succeed():
    """Concurrent create-on-first-write must not wound the losing racers.

    Exactly one thread creates the table; the rest fall through to the ordinary
    append path while the winner may still hold the namespace lock.  All of
    them are authorized and must commit.
    """
    SuperTable(SUPER, ORG)

    targets = [
        ("race_new", 2000 + index * ROWS_PER_WRITE) for index in range(THREADS)
    ]
    errors = _run_parallel(targets)

    assert errors == [], (
        "a racing first-writer was denied: "
        + "; ".join(f"{type(e).__name__}: {e}" for e in errors)
    )
    assert _row_count("race_new") == ROWS_PER_WRITE * THREADS
