"""Regression tests for DataWriter's durable reconciliation boundary."""

from __future__ import annotations

import pytest

from supertable.tests.test_data_writer_comprehensive import (
    _simple_arrow,
    fake_catalog,
    fake_monitor,
    fake_storage,
    writer,
)


class AlreadyReconciled(RuntimeError):
    """Caller-owned control signal used by a durable write retry."""


def test_reconciliation_callback_rechecks_under_exact_table_lock(
    writer, fake_catalog,
):
    lock_observations: list[bool] = []

    def reconcile() -> None:
        lock_observations.append(bool(fake_catalog._locks))

    writer.write(
        "admin",
        "t1",
        _simple_arrow(1),
        overwrite_columns=[],
        reconciliation_callback=reconcile,
    )

    assert lock_observations == [False, True]
    assert fake_catalog._locks == {}
    assert len(fake_catalog.set_leaf_payload_cas_calls) == 1


def test_under_lock_reconciliation_signal_prevents_io_and_publication(
    writer, fake_catalog,
):
    calls = 0

    def reconcile() -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            assert fake_catalog._locks
            raise AlreadyReconciled("exact lineage was already committed")

    with pytest.raises(AlreadyReconciled, match="already committed"):
        writer.write(
            "admin",
            "t1",
            _simple_arrow(1),
            overwrite_columns=[],
            reconciliation_callback=reconcile,
        )

    assert calls == 2
    assert not writer._mocks["process"].called
    assert not writer._mocks["simple_inst"].update.called
    assert fake_catalog.set_leaf_payload_cas_calls == []
    assert fake_catalog.bump_root_calls == []
    assert len(fake_catalog.release_calls) == 1
    assert fake_catalog._locks == {}
