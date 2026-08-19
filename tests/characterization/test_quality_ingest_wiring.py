"""The write path must actually notify the Data Quality scheduler.

Regression seal for a latent bug.  The writer used to do::

    from supertable.services.quality.scheduler import notify_ingest

— a module path that *never existed* — wrapped in ``except Exception: pass``.
The ``ModuleNotFoundError`` was swallowed silently, so the post-ingest DQ
trigger was a dead no-op everywhere.  The quality package now lives inside the
library (``supertable.quality``); DataWriter hands off a durable unresolved
generation inside the snapshot commit, while the direct producer API remains
available from that package.

These tests prove, end to end against the hermetic fake Redis (see
``tests/conftest.py``):

  * the public producer module path resolves (canary for the original bug);
  * a real write commits durable unresolved work for that table;
  * ``notify_ingest`` stays a no-op for system tables and when post-ingest /
    quality is disabled (so it never floods Redis or blocks a write);
  * ``start_scheduler`` is idempotent — one daemon thread, re-entrant-safe.
"""
from __future__ import annotations

import importlib
import threading
import uuid

import pyarrow as pa

from supertable.data_writer import DataWriter
from supertable.quality import scheduler as dq_scheduler
from supertable.quality.config import DQConfig
from supertable.quality.scheduler import (
    _pending_key,
    _unresolved_pending_key,
    notify_ingest,
    start_scheduler,
)
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "demo"
ROLE = "superadmin"
KEY = "grp"


def _rows(keys) -> pa.Table:
    return pa.table({KEY: list(keys), "amount": [0] * len(keys)})


def test_public_ingest_notification_path_resolves():
    """Canary: the public module path must exist and expose ``notify_ingest``.

    This is the specific path that was broken (``supertable.services.quality``)
    and silently swallowed for so long.  ``supertable.quality`` re-exports it
    too, mirroring how the writer reaches it."""
    mod = importlib.import_module("supertable.quality.scheduler")
    assert hasattr(mod, "notify_ingest")
    pkg = importlib.import_module("supertable.quality")
    assert pkg.notify_ingest is mod.notify_ingest


def test_write_sets_durable_unresolved_generation(hermetic_fakeredis):
    """A real append atomically leaves durable scheduler work behind."""
    fake = hermetic_fakeredis
    simple = f"dq_wire_{uuid.uuid4().hex[:8]}"
    SuperTable(SUPER, ORG)  # bootstrap super + default superadmin role
    dw = DataWriter(super_name=SUPER, organization=ORG)
    # The writer must be talking to the same fake Redis we inspect.
    assert dw.catalog.r is fake

    key = _unresolved_pending_key(ORG, SUPER, simple)
    assert fake.get(key) is None, "no unresolved generation before any write"

    dw.write(role_name=ROLE, simple_name=simple, data=_rows(["a", "b"]),
             overwrite_columns=[])

    generation = fake.get(key)
    assert generation is not None, "write must persist unresolved DQ work"
    assert generation == dw.catalog.get_leaf(ORG, SUPER, simple)["commit_id"]
    # This marker is the crash-safe hand-off. The scheduler later resolves it
    # into TTL-bounded scalar/per-mode pending keys after reading configuration.
    assert fake.ttl(key) == -1


def test_repeated_writes_keep_single_unresolved_generation(hermetic_fakeredis):
    """Debounce: many writes collapse to one overwrite-only generation."""
    fake = hermetic_fakeredis
    simple = f"dq_debounce_{uuid.uuid4().hex[:8]}"
    SuperTable(SUPER, ORG)
    dw = DataWriter(super_name=SUPER, organization=ORG)
    key = _unresolved_pending_key(ORG, SUPER, simple)

    generations = []
    for k in ["a", "b", "c", "d", "e"]:
        dw.write(role_name=ROLE, simple_name=simple, data=_rows([k]),
                 overwrite_columns=[])
        generations.append(fake.get(key))

    # One unresolved key for the table — not one per write.
    matches = fake.keys(_unresolved_pending_key(ORG, SUPER, "*"))
    assert matches == [key], matches
    assert len(set(generations)) == 5
    assert generations[-1] == dw.catalog.get_leaf(
        ORG, SUPER, simple,
    )["commit_id"]


def test_system_table_is_skipped(hermetic_fakeredis):
    """Quality checks write ``__...__`` system tables themselves; notifying on
    those would loop, so they are skipped."""
    fake = hermetic_fakeredis
    notify_ingest(fake, ORG, SUPER, "__data_quality__")
    assert fake.get(_pending_key(ORG, SUPER, "__data_quality__")) is None


def test_system_table_write_does_not_commit_unresolved_work(hermetic_fakeredis):
    fake = hermetic_fakeredis
    table = f"__dq_{uuid.uuid4().hex[:8]}__"
    SuperTable(SUPER, ORG)
    DataWriter(super_name=SUPER, organization=ORG).write(
        role_name=ROLE,
        simple_name=table,
        data=_rows(["a"]),
        overwrite_columns=[],
    )

    assert fake.get(_unresolved_pending_key(ORG, SUPER, table)) is None


def test_disabled_post_ingest_skips(hermetic_fakeredis):
    """``post_ingest: False`` in the schedule means no pending flag is set."""
    fake = hermetic_fakeredis
    table = "sales"
    SuperTable(SUPER, ORG)
    DQConfig(fake, ORG, SUPER).set_schedule({"post_ingest": False, "enabled": True})
    notify_ingest(fake, ORG, SUPER, table)
    assert fake.get(_pending_key(ORG, SUPER, table)) is None


def test_disabled_quality_skips(hermetic_fakeredis):
    """Quality fully disabled (``enabled: False``) means no pending flag."""
    fake = hermetic_fakeredis
    table = "sales"
    SuperTable(SUPER, ORG)
    DQConfig(fake, ORG, SUPER).set_schedule({"post_ingest": True, "enabled": False})
    notify_ingest(fake, ORG, SUPER, table)
    assert fake.get(_pending_key(ORG, SUPER, table)) is None


def test_start_scheduler_is_idempotent(monkeypatch):
    """``start_scheduler`` launches exactly one daemon thread and a second call
    is a no-op while that thread is alive."""
    # Reset the module singleton so this test is order-independent; monkeypatch
    # restores it on teardown.
    monkeypatch.setattr(dq_scheduler, "_scheduler_thread", None)

    stop = threading.Event()
    started = threading.Event()

    def _fake_loop():
        started.set()
        stop.wait(5)  # stay alive until the test releases us

    # The real loop sleeps 10s then ticks Redis every 60s; swap it for a cheap
    # stand-in so we exercise the start/idempotency logic, not the loop body.
    monkeypatch.setattr(dq_scheduler, "_scheduler_loop", _fake_loop)

    try:
        assert start_scheduler() is True, "first call starts the thread"
        assert started.wait(2), "the daemon thread should have started"
        assert start_scheduler() is False, "second call is a no-op while alive"

        t = dq_scheduler._scheduler_thread
        assert t is not None and t.is_alive()
        assert t.daemon, "scheduler thread must be a daemon (dies with process)"
        assert t.name == "supertable-dq-scheduler"
    finally:
        stop.set()
        t = dq_scheduler._scheduler_thread
        if t is not None:
            t.join(2)
