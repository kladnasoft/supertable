from __future__ import annotations

import importlib
import threading
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pytest

import supertable.data_reader as data_reader
import supertable.rbac.access_control as access_control
from supertable.data_classes import Reflection, SuperSnapshot
from supertable.engine.engine_enum import Engine
from supertable.engine.island_resources import (
    ArrowBatchStream,
    ResourceReservationCancelled,
)


ROLE_A = "a" * 64
ROLE_B = "b" * 64
data_estimator = importlib.import_module("supertable.engine.data_estimator")


def test_expected_policy_fingerprints_are_strict_lowercase_sha256(monkeypatch):
    monkeypatch.setattr(
        data_reader,
        "DataReader",
        lambda **_kwargs: pytest.fail("malformed pin reached DataReader"),
    )

    for malformed in ("", "a" * 63, "A" * 64, "g" * 64, 7):
        with pytest.raises(ValueError, match="64 lowercase hexadecimal"):
            data_reader.query_sql_stream(
                "org",
                "shop",
                "SELECT 1",
                Engine.AUTO,
                "reader",
                max_total_rows=1,
                timeout_sec=1,
                expected_role_policy_fingerprint=malformed,
            )


def test_restrict_read_access_compares_same_resolved_role_context(monkeypatch):
    calls = 0

    class StaticRoleManager:
        def __init__(self, **_kwargs):
            pass

        def get_role_by_name(self, _role_name):
            nonlocal calls
            calls += 1
            return {
                "role_id": "role-1",
                "role": "reader",
                "enabled": True,
                "tables": {"*": {"columns": ["*"], "filters": ["*"]}},
            }

    monkeypatch.setattr(access_control, "RoleManager", StaticRoleManager)
    context = access_control.resolve_role_access_context(
        "shop",
        "org",
        "reader",
        permission=access_control.Permission.READ,
        label="read",
    )
    calls = 0
    policy_out = {}

    views = access_control.restrict_read_access(
        "shop",
        "org",
        "reader",
        [],
        [],
        expected_role_policy_fingerprint=context.fingerprint,
        policy_fingerprints_out=policy_out,
    )

    assert views == {}
    assert calls == 1
    assert policy_out == {"shop": context.fingerprint}

    with pytest.raises(PermissionError, match="Role policy changed"):
        access_control.restrict_read_access(
            "shop",
            "org",
            "reader",
            [],
            [],
            expected_role_policy_fingerprint=ROLE_B,
        )


def test_linked_share_policy_seal_binds_provenance_columns_and_schema_not_urls():
    base = {
        "_linked_share": "link-1",
        "_provider_org": "provider-a",
        "_allowed_columns": ["id"],
        "resources": [{"file": "https://one.invalid/a?token=secret"}],
        "expires_ms": 1,
    }
    first, allowed = data_estimator._linked_share_policy_state(
        base,
        schema={"id": "BIGINT"},
    )
    refreshed, _ = data_estimator._linked_share_policy_state(
        {
            **base,
            "resources": [{"file": "https://two.invalid/a?token=rotated"}],
            "expires_ms": 999,
        },
        schema={"id": "BIGINT"},
    )
    changed_provider, _ = data_estimator._linked_share_policy_state(
        {**base, "_provider_org": "provider-b"},
        schema={"id": "BIGINT"},
    )
    changed_schema, _ = data_estimator._linked_share_policy_state(
        {**base, "_allowed_columns": ["id", "name"]},
        schema={"id": "BIGINT", "name": "VARCHAR"},
    )

    assert allowed == ["id"]
    assert first == refreshed
    assert first != changed_provider
    assert first != changed_schema
    assert access_control.validate_policy_fingerprint(first) == first


def test_linked_share_policy_seal_rejects_missing_explicit_column_policy():
    with pytest.raises(RuntimeError, match="column policy is unavailable"):
        data_estimator._linked_share_policy_state(
            {
                "_linked_share": "link-1",
                "_provider_org": "provider-a",
            },
            schema={"id": "BIGINT", "secret": "VARCHAR"},
        )


def test_linked_provider_generation_is_pinned_and_must_be_unambiguous():
    wrapper = {
        "payload": {"_linked_provider_generated_ms": 123_456},
    }
    snapshot = {"_linked_provider_generated_ms": 123_456}
    assert data_estimator._linked_share_publication_generation(
        wrapper, snapshot, linked=True,
    ) == 123_456

    with pytest.raises(RuntimeError, match="publication generation"):
        data_estimator._linked_share_publication_generation(
            wrapper,
            {"_linked_provider_generated_ms": 123_457},
            linked=True,
        )
    with pytest.raises(RuntimeError, match="unavailable"):
        data_estimator._linked_share_publication_generation(
            {}, linked=True,
        )
    with pytest.raises(RuntimeError, match="invalid"):
        data_estimator._linked_share_publication_generation(
            {"_linked_provider_generated_ms": True}, linked=True,
        )
    with pytest.raises(RuntimeError, match="no linked-share identity"):
        data_estimator._linked_share_publication_generation(
            snapshot, linked=False,
        )


@pytest.mark.parametrize(
    "provider",
    [None, "", "\x00provider", "p" * 1025],
)
def test_linked_share_policy_seal_rejects_missing_or_invalid_provider(provider):
    policy = {
        "_linked_share": "link-1",
        "_allowed_columns": ["id"],
    }
    if provider is not None:
        policy["_provider_org"] = provider
    with pytest.raises(RuntimeError):
        data_estimator._linked_share_policy_state(
            policy,
            schema={"id": "BIGINT"},
        )


@pytest.mark.parametrize(
    ("policy_update", "schema"),
    [
        ({"_linked_share": "l" * 1025}, {"id": "BIGINT"}),
        ({"_allowed_columns": ["c" * 1025]}, {"id": "BIGINT"}),
        ({}, {"bad\x00name": "BIGINT"}),
        ({}, {"id": "T" * (64 * 1024 + 1)}),
    ],
)
def test_linked_share_policy_seal_bounds_policy_and_schema_text(
    policy_update, schema,
):
    policy = {
        "_linked_share": "link-1",
        "_provider_org": "provider-a",
        "_allowed_columns": ["id"],
        **policy_update,
    }
    with pytest.raises(RuntimeError, match="size limit|invalid"):
        data_estimator._linked_share_policy_state(policy, schema=schema)


def test_effective_policy_fingerprint_binds_canonical_share_filter_and_view():
    def fingerprint(raw_filter: str, allowed_columns):
        canonical = data_reader._validated_share_row_filter(raw_filter)
        snapshot = SuperSnapshot(
            "shop",
            "events",
            1,
            share_row_filter=canonical,
        )
        reflection = Reflection(
            "local",
            0,
            0,
            [snapshot],
            rbac_views={
                "events": data_reader.RbacViewDef(
                    allowed_columns=list(allowed_columns),
                    where_clause=canonical,
                ),
            },
        )
        return data_reader._effective_read_policy_fingerprint(
            {"shop": ROLE_A}, reflection,
        )

    baseline = fingerprint("tenant_id=7", ["id"])

    assert baseline == fingerprint(" tenant_id = 7 ", ["ID"])
    assert baseline != fingerprint("tenant_id = 8", ["id"])
    assert baseline != fingerprint("tenant_id = 7", ["id", "name"])


def _install_reader_preflight(
    monkeypatch,
    *,
    snapshot: SuperSnapshot,
    executor_factory,
):
    monkeypatch.setattr(data_reader, "get_storage", lambda: MagicMock())
    monkeypatch.setattr(
        data_reader.DataReader,
        "_assert_targets_exist",
        lambda _self, _tables: None,
    )

    def authorize(**kwargs):
        kwargs["policy_fingerprints_out"].update({"shop": ROLE_A})
        expected = kwargs.get("expected_role_policy_fingerprint")
        if expected is not None and expected != ROLE_A:
            raise PermissionError("Role policy changed before query execution")
        return {}

    monkeypatch.setattr(data_reader, "restrict_read_access", authorize)
    reflection = Reflection(
        storage_type="LocalStorage",
        reflection_bytes=1,
        total_reflections=1,
        supers=[snapshot],
    )
    estimator = MagicMock()
    estimator.estimate.return_value = reflection
    monkeypatch.setattr(data_reader, "DataEstimator", lambda **_kwargs: estimator)
    monkeypatch.setattr(data_reader, "Executor", executor_factory)
    monkeypatch.setattr(
        data_reader,
        "QueryPlanManager",
        lambda **kwargs: SimpleNamespace(
            query_id="qid",
            query_hash="qhash",
            requested_engine="",
            engine_forced=False,
            query_observation_store=None,
            source_type="",
            original_table="",
            organization=kwargs["organization"],
            super_name=kwargs["super_name"],
        ),
    )
    monkeypatch.setattr(data_reader, "extend_execution_plan", lambda **_kwargs: None)
    monkeypatch.setattr(
        data_reader,
        "settings",
        replace(
            data_reader.settings,
            SUPERTABLE_READ_PRUNING_ENABLED=False,
        ),
    )
    return reflection


def test_effective_policy_mismatch_never_constructs_executor(monkeypatch):
    constructed = False

    def forbidden_executor(**_kwargs):
        nonlocal constructed
        constructed = True
        pytest.fail("effective-policy mismatch reached Executor")

    snapshot = SuperSnapshot(
        super_name="shop",
        simple_name="events",
        simple_version=1,
        files=["events.parquet"],
        columns={"id"},
    )
    _install_reader_preflight(
        monkeypatch,
        snapshot=snapshot,
        executor_factory=forbidden_executor,
    )
    reader = data_reader.DataReader(
        "shop", "org", "SELECT id FROM events",
    )

    with pytest.raises(PermissionError, match="Effective read policy changed"):
        reader.execute(
            "reader",
            engine=Engine.AUTO,
            expected_role_policy_fingerprint=ROLE_A,
            expected_effective_policy_fingerprint=ROLE_B,
        )

    assert constructed is False


def test_share_columns_are_enforced_and_effective_fingerprint_is_emitted(
    monkeypatch,
):
    share_seal, allowed = data_estimator._linked_share_policy_state(
        {
            "_linked_share": "link-1",
            "_provider_org": "provider",
            "_allowed_columns": ["id"],
        },
        schema={"id": "BIGINT"},
    )
    snapshot = SuperSnapshot(
        super_name="shop",
        simple_name="events",
        simple_version=1,
        files=["events.parquet"],
        columns={"id"},
        share_row_filter="tenant_id=7",
        share_policy_fingerprint=share_seal,
        share_allowed_columns=allowed,
    )
    executor = MagicMock()
    executor.execute.return_value = (pd.DataFrame({"id": [1]}), "duckdb")
    reflection = _install_reader_preflight(
        monkeypatch,
        snapshot=snapshot,
        executor_factory=lambda **_kwargs: executor,
    )
    reader = data_reader.DataReader(
        "shop", "org", "SELECT * FROM events",
    )

    result, status, _message = reader.execute("reader", engine=Engine.DUCKDB)

    assert status is data_reader.Status.OK
    assert result.to_dict("records") == [{"id": 1}]
    protected = reflection.rbac_views["events"]
    assert protected.allowed_columns == ["id"]
    assert protected.where_clause == "tenant_id = 7"
    assert access_control.validate_policy_fingerprint(
        reader.effective_policy_fingerprint
    ) == reader.effective_policy_fingerprint
    out = {}
    data_reader._update_query_out(reader, out, requested_engine=Engine.DUCKDB)
    assert out["role_policy_fingerprint"] == ROLE_A
    assert out["effective_policy_fingerprint"] == (
        reader.effective_policy_fingerprint
    )


def test_policy_preflight_helper_never_constructs_executor(monkeypatch):
    snapshot = SuperSnapshot(
        super_name="shop",
        simple_name="events",
        simple_version=1,
        files=["events.parquet"],
        columns={"id"},
    )
    _install_reader_preflight(
        monkeypatch,
        snapshot=snapshot,
        executor_factory=lambda **_kwargs: pytest.fail("Executor was constructed"),
    )

    fingerprint = data_reader.query_sql_policy_fingerprint(
        "org",
        "shop",
        "SELECT id FROM events",
        Engine.AUTO,
        "reader",
        timeout_sec=1,
        expected_role_policy_fingerprint=ROLE_A,
    )

    assert access_control.validate_policy_fingerprint(fingerprint) == fingerprint


def test_row_budget_closed_waits_for_blocked_inner_stream_to_finalize():
    entered = threading.Event()
    release = threading.Event()
    schema = pa.schema([("id", pa.int64())])

    def producer():
        entered.set()
        release.wait(timeout=5)
        yield pa.record_batch([[1]], schema=schema)

    inner = ArrowBatchStream(schema, producer())
    stream = data_reader._RowBudgetResultStream(inner, 10)
    failures = []

    def consume():
        try:
            next(stream)
        except BaseException as exc:  # expected cooperative cancellation
            failures.append(exc)

    worker = threading.Thread(target=consume)
    worker.start()
    assert entered.wait(timeout=2)
    stream.cancel()

    assert stream.closed is False
    assert inner.closed is False
    release.set()
    worker.join(timeout=2)
    assert not worker.is_alive()
    assert stream.closed is True
    assert any(isinstance(exc, ResourceReservationCancelled) for exc in failures)


@pytest.mark.parametrize(
    "predicate",
    [
        "current_setting('secret') = 'x'",
        "read_text('/etc/passwd') = 'x'",
        "nextval('sequence_name') > 0",
        "random() > 0.5",
    ],
)
def test_linked_share_filter_rejects_settings_io_sequence_and_random_functions(
    predicate,
):
    with pytest.raises(RuntimeError, match="unavailable function"):
        data_reader._validated_share_row_filter(predicate)


def test_linked_share_filter_allows_small_deterministic_scalar_subset():
    predicate = data_reader._validated_share_row_filter(
        "lower(trim(name)) = 'alice' AND coalesce(score, 0) >= 1"
    )

    assert "LOWER" in predicate
    assert "COALESCE" in predicate
