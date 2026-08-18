"""Integration tests for the transactional privileged RBAC audit ledger.

Unlike ``test_rbac.py`` these tests register and execute the production Lua
sources with fakeredis.  This matters for the fail-closed boundary: a Python
script double cannot prove that the audit append and RBAC mutation share one
Redis transaction or that Redis/cjson preserves the intended envelope.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Iterator

import fakeredis
import pytest
from redis.exceptions import ResponseError

from supertable import redis_keys as RK
from supertable.audit.privileged import (
    PrivilegedActionContext,
    PrivilegedAuditRecord,
)
from supertable.audit.privileged_outbox import PrivilegedAuditOutbox
from supertable.audit.privileged_worker import (
    ActivationBaselineReport,
    attest_activation_baseline,
    compute_privileged_state_sha256,
)
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.user_manager import UserManager
from supertable.redis_catalog import (
    RbacAuditAttemptError,
    RbacAuditConditionConflict,
    RbacDecisionError,
    RbacIntegrityError,
    RedisCatalog,
)


ORG = "privileged-audit-org"
SUP = "privileged-audit-super"

_RBAC_SCRIPT_ATTRIBUTES = (
    "_rbac_validate_meta",
    "_rbac_append_attempt",
    "_rbac_create_role",
    "_rbac_create_user",
    "_rbac_update_role",
    "_rbac_update_user",
    "_rbac_delete_user",
    "_rbac_delete_role",
    "_rbac_remove_role_from_user",
    "_rbac_add_role_to_user",
    "_auth_create_token",
    "_auth_delete_token",
    "_begin_namespace_deletion",
    "_delete_namespace_batch",
    "_finalize_namespace_deletion",
)


@pytest.fixture
def catalog() -> RedisCatalog:
    """Build a catalog around real registered Lua without a live connector."""

    instance = RedisCatalog.__new__(RedisCatalog)
    instance.r = fakeredis.FakeStrictRedis(decode_responses=True)
    for attribute in _RBAC_SCRIPT_ATTRIBUTES:
        source = getattr(instance, "_LUA" + attribute.upper())
        setattr(instance, attribute, instance.r.register_script(source))
    outbox = _outbox(instance)
    state_sha256 = compute_privileged_state_sha256(outbox, ORG)
    assert attest_activation_baseline(
        outbox,
        ActivationBaselineReport(
            organization=ORG,
            activation_id="test-activation",
            created_ms=1_700_000_000_000,
            state_sha256=state_sha256,
            artifact_sha256="a" * 64,
        ),
    )
    return instance


def _role(
    role_id: str,
    *,
    role_type: str = "reader",
    role_name: str | None = None,
) -> Dict[str, Any]:
    return {
        "role_id": role_id,
        "role": role_type,
        "role_name": role_name or role_id,
        "tables": {
            "orders": {
                "columns": ["id", "account_id"],
                "filters": ["*"],
            },
        },
    }


def _contexts() -> Iterator[PrivilegedActionContext]:
    sequence = 0
    while True:
        sequence += 1
        yield PrivilegedActionContext(
            actor_type="user",
            actor_id="security-admin-id",
            username="security.admin",
            ip="192.0.2.10",
            user_agent="rbac-integration-test",
            correlation_id="correlation-42",
            session_id="session-7",
            server="control-plane-1",
            reason="approved access maintenance",
            ticket_id="SEC-4242",
            mutation_id=f"mutation-{sequence}",
            cause="admin_api",
        )


def _outbox(catalog: RedisCatalog) -> PrivilegedAuditOutbox:
    return PrivilegedAuditOutbox(
        catalog.r,
        stream_key=RK.audit_privileged_outbox(ORG),
        delivery_ledger_key=RK.audit_privileged_delivery(ORG),
    )


def _managers(catalog: RedisCatalog) -> tuple[RoleManager, UserManager]:
    role_manager = RoleManager.__new__(RoleManager)
    role_manager.organization = ORG
    role_manager.super_name = SUP
    role_manager._catalog = catalog
    user_manager = UserManager.__new__(UserManager)
    user_manager.organization = ORG
    user_manager.super_name = SUP
    user_manager._catalog = catalog
    return role_manager, user_manager


def _race_conditional_append(catalog: RedisCatalog, mutation) -> None:
    """Run ``mutation`` immediately inside the observed-state/XADD gap."""

    original = catalog._rbac_append_attempt

    def racing_append(*, keys, args):
        catalog._rbac_append_attempt = original
        mutation()
        return original(keys=keys, args=args)

    catalog._rbac_append_attempt = racing_append


def _rbac_role_state(catalog: RedisCatalog, role_id: str) -> Dict[str, Any]:
    """Capture every Redis structure an update of ``role_id`` may change."""

    document = catalog.r.hgetall(RK.rbac_role_doc(ORG, SUP, role_id))
    role_type = document.get("role", "reader")
    return {
        "document": document,
        "index": catalog.r.smembers(RK.rbac_role_index(ORG, SUP)),
        "type_index": catalog.r.smembers(
            RK.rbac_role_type_index(ORG, SUP, role_type)
        ),
        "name_map": catalog.r.hgetall(RK.rbac_rolename_to_id(ORG, SUP)),
        "meta": catalog.r.hgetall(RK.rbac_role_meta(ORG, SUP)),
    }


def test_super_table_data_cleanup_cannot_bypass_the_rbac_ledger(
    catalog: RedisCatalog,
) -> None:
    """Generic namespace deletion retains all security-control state."""

    context = next(_contexts())
    catalog.rbac_create_role(
        ORG,
        SUP,
        "retained-role",
        _role("retained-role"),
        action_context=context,
    )
    # Namespace deletion requires the same structurally valid root document as
    # every production catalog mutation.  A bare sentinel string would model
    # catalog corruption, which must fail closed before deletion begins.
    catalog.r.set(
        RK.meta_root(ORG, SUP),
        json.dumps({"version": 0, "ts": 1}),
    )
    sequence_before = catalog.r.hget(
        RK.audit_privileged_meta(ORG), "sequence"
    )

    namespace_token = "namespace-delete-token"
    catalog.r.set(RK.lock_namespace(ORG, SUP), namespace_token)
    intent = catalog.begin_namespace_deletion(
        ORG,
        SUP,
        namespace_token=namespace_token,
    )
    try:
        deleted = catalog.delete_super_table(
            ORG,
            SUP,
            namespace_token=namespace_token,
            intent_id=intent["intent_id"],
        )
    finally:
        if catalog.r.get(RK.lock_namespace(ORG, SUP)) == namespace_token:
            catalog.r.delete(RK.lock_namespace(ORG, SUP))

    assert deleted == 1
    assert catalog.r.get(RK.meta_root(ORG, SUP)) is None
    assert catalog.get_namespace_deletion_intent(ORG, SUP)["status"] == "deleted"
    assert catalog.rbac_role_exists(ORG, SUP, "retained-role")
    assert catalog.r.hget(
        RK.audit_privileged_meta(ORG), "sequence"
    ) == sequence_before


def test_standalone_denied_attempt_advances_only_the_shared_audit_ledger(
    catalog: RedisCatalog,
) -> None:
    context = next(_contexts())
    role_meta = RK.rbac_role_meta(ORG, SUP)
    role_index = RK.rbac_role_index(ORG, SUP)

    catalog.rbac_append_attempt(
        ORG,
        SUP,
        action="role_update",
        resource_type="role",
        resource_id="missing-role",
        namespace="role",
        outcome="denied",
        cause="request_rejected",
        action_context=context,
    )

    assert catalog.r.exists(role_meta) == 0
    assert catalog.r.exists(role_index) == 0
    entry = _outbox(catalog).query(count=1)[0]
    assert entry.event["outcome"] == "denied"
    assert entry.event["cause"] == "request_rejected"
    assert entry.event["ledger_sequence"] == 1
    assert entry.event["namespace_version"] == 0
    assert entry.event["affected_count"] == 0
    assert entry.event["mutation_id"] == context.mutation_id
    assert catalog.r.hget(RK.audit_privileged_meta(ORG), "sequence") == "1"


def test_privileged_mutation_is_fenced_until_live_baseline_is_anchored(
    catalog: RedisCatalog,
) -> None:
    catalog.r.delete(RK.audit_privileged_activation(ORG))

    with pytest.raises(ResponseError, match="baseline is not anchored"):
        catalog.rbac_create_role(
            ORG,
            SUP,
            "must-not-exist",
            _role("must-not-exist"),
            action_context=next(_contexts()),
        )

    assert not catalog.r.exists(RK.rbac_role_doc(ORG, SUP, "must-not-exist"))
    assert catalog.r.xlen(RK.audit_privileged_outbox(ORG)) == 0


def test_standalone_attempt_records_failure_despite_corrupt_namespace(
    catalog: RedisCatalog,
) -> None:
    success_json = catalog._rbac_audit_json(
        action_context=next(_contexts()),
        org=ORG,
        sup=SUP,
        action="role_update",
        resource_type="role",
        resource_id="reader",
        before_document=None,
        after_document=None,
        before_version=0,
        after_version=0,
    )
    with pytest.raises(ResponseError, match="successful privileged events"):
        catalog._rbac_append_attempt(
            keys=[RK.rbac_role_meta(ORG, SUP)] + catalog._rbac_audit_keys(ORG),
            args=[ORG, SUP, success_json],
        )
    assert catalog.r.xlen(RK.audit_privileged_outbox(ORG)) == 0

    catalog.r.hset(RK.rbac_role_meta(ORG, SUP), "version", "01")
    catalog.rbac_append_attempt(
        ORG,
        SUP,
        action="role_update",
        resource_type="role",
        resource_id="reader",
        namespace="role",
        outcome="failure",
        cause="state_integrity_error",
        action_context=next(_contexts()),
        severity="critical",
    )
    assert catalog.r.xlen(RK.audit_privileged_outbox(ORG)) == 1
    event = _outbox(catalog).query(count=1)[0].event
    assert event["outcome"] == "failure"
    assert event["cause"] == "state_integrity_error"
    assert event["namespace_version"] == 0


def test_auth_token_create_delete_and_no_change_are_durable_and_typed(
    catalog: RedisCatalog,
) -> None:
    contexts = _contexts()
    created = catalog.create_auth_token(
        ORG,
        "security.admin",
        label="automation",
        action_context=next(contexts),
    )
    token_id = created["token_id"]
    assert catalog.validate_auth_token(ORG, created["token"])

    assert catalog.delete_auth_token(
        ORG, token_id, action_context=next(contexts),
    ) is True
    assert catalog.delete_auth_token(
        ORG, token_id, action_context=next(contexts),
    ) is False
    with pytest.raises(ValueError, match="SHA-256"):
        catalog.delete_auth_token(
            ORG, "not-a-token-id", action_context=next(contexts),
        )

    events = [
        entry.event
        for entry in _outbox(catalog).query(count=10, newest_first=False)
    ]
    assert [event["action"] for event in events] == [
        "token_create", "token_delete", "token_delete", "token_delete",
    ]
    assert [event["outcome"] for event in events] == [
        "success", "success", "no_change", "denied",
    ]
    assert all(event["actor_id"] == "security-admin-id" for event in events)
    assert all(event["super_name"] == "_organization_" for event in events)
    assert catalog.r.hget(
        catalog._auth_token_meta_key(ORG), "version",
    ) == "2"


def test_auth_token_mutation_requires_context_and_fails_closed_with_audit_down(
    catalog: RedisCatalog,
) -> None:
    with pytest.raises(TypeError):
        catalog.create_auth_token(ORG, "security.admin")
    assert catalog.r.hlen(RK.auth_tokens(ORG)) == 0

    with pytest.raises(ValueError, match="explicit actor context"):
        catalog.create_auth_token(
            ORG, "security.admin", action_context=None,
        )
    denied = _outbox(catalog).query(count=1)[0].event
    assert denied["outcome"] == "denied"
    assert denied["cause"] == "missing_actor_context"
    assert denied["context_missing"] is True

    catalog.r.delete(
        RK.audit_privileged_outbox(ORG), RK.audit_privileged_meta(ORG),
    )
    catalog.r.set(RK.audit_privileged_outbox(ORG), "wrong-type")
    with pytest.raises(ResponseError, match="outbox has wrong Redis type"):
        catalog.create_auth_token(
            ORG,
            "security.admin",
            action_context=next(_contexts()),
        )
    assert catalog.r.hlen(RK.auth_tokens(ORG)) == 0


def test_conditional_membership_scan_is_hard_bounded_before_xadd(
    catalog: RedisCatalog,
) -> None:
    user_key = RK.rbac_user_doc(ORG, SUP, "oversized-user")
    catalog.r.hset(
        user_key,
        mapping={
            "roles": "[" + ("\"role\"," * 11_000) + "\"role\"]",
            "doc_version": "1",
        },
    )

    with pytest.raises(RbacAuditAttemptError):
        catalog.rbac_append_attempt(
            ORG,
            SUP,
            action="user_role_assign",
            resource_type="user_role_assignment",
            resource_id="oversized-user:role",
            namespace="user",
            outcome="no_change",
            cause="role_already_assigned",
            action_context=next(_contexts()),
            conditions=[{
                "kind": "assignment_membership",
                "user_id": "oversized-user",
                "role_id": "role",
                "present": True,
                "version": "1",
            }],
        )

    assert catalog.r.xlen(RK.audit_privileged_outbox(ORG)) == 0


def test_condition_miss_fails_closed_when_failure_evidence_cannot_append(
    catalog: RedisCatalog,
) -> None:
    calls = 0

    def unavailable_append(*, keys, args):
        nonlocal calls
        calls += 1
        if calls == 1:
            return 0
        raise ResponseError("audit stream unavailable")

    catalog._rbac_append_attempt = unavailable_append
    with pytest.raises(RbacAuditAttemptError):
        catalog.rbac_append_attempt(
            ORG,
            SUP,
            action="role_delete",
            resource_type="role",
            resource_id="missing-role",
            namespace="role",
            outcome="no_change",
            cause="resource_missing",
            action_context=next(_contexts()),
            conditions=[{"kind": "resource_absent"}],
        )

    assert calls == 2
    assert catalog.r.xlen(RK.audit_privileged_outbox(ORG)) == 0


def test_resource_fields_condition_appends_when_observation_is_still_true(
    catalog: RedisCatalog,
) -> None:
    user_key = RK.rbac_user_doc(ORG, SUP, "stable-user")
    catalog.r.hset(
        user_key,
        mapping={"username": "stable", "doc_version": "7", "roles": "[]"},
    )

    catalog.rbac_append_attempt(
        ORG,
        SUP,
        action="user_update",
        resource_type="user",
        resource_id="stable-user",
        namespace="user",
        outcome="no_change",
        cause="empty_update",
        action_context=next(_contexts()),
        before_version=7,
        conditions=[{
            "kind": "resource_fields",
            "fields": {"username": "stable", "doc_version": "7"},
        }],
    )

    events = [
        entry.event
        for entry in _outbox(catalog).query(count=10, newest_first=False)
    ]
    assert [event["outcome"] for event in events] == ["no_change"]
    assert [event["cause"] for event in events] == ["empty_update"]


@pytest.mark.parametrize(
    ("action", "resource_type", "resource_id", "namespace", "cause", "condition"),
    (
        (
            "role_delete", "role", "reader", "role", "resource_missing",
            {
                "kind": "resource_absent",
                "key": RK.rbac_role_doc("other-org", "other-super", "reader"),
            },
        ),
        (
            "user_delete", "user", "alice", "user", "resource_missing",
            {"kind": "role_absent", "role_id": "reader"},
        ),
        (
            "user_role_assign", "user_role_assignment", "alice:reader",
            "user", "user_missing",
            {
                "kind": "assignment_user_absent",
                "user_id": "mallory",
                "role_id": "reader",
            },
        ),
        (
            "token_delete", "auth_token", "a" * 64, "token",
            "resource_missing",
            {"kind": "token_absent", "token_id": "b" * 64},
        ),
    ),
)
def test_public_attempt_conditions_cannot_cross_scope_action_or_resource(
    catalog: RedisCatalog,
    action: str,
    resource_type: str,
    resource_id: str,
    namespace: str,
    cause: str,
    condition: Dict[str, Any],
) -> None:
    super_name = "_organization_" if namespace == "token" else SUP
    with pytest.raises(ValueError, match="condition"):
        catalog.rbac_append_attempt(
            ORG,
            super_name,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            namespace=namespace,
            outcome="no_change",
            cause=cause,
            action_context=next(_contexts()),
            conditions=[condition],
        )

    assert catalog.r.xlen(RK.audit_privileged_outbox(ORG)) == 0


def test_public_attempt_rejects_non_string_identity_and_condition_kind(
    catalog: RedisCatalog,
) -> None:
    with pytest.raises(ValueError, match="identity"):
        catalog.rbac_append_attempt(
            ORG,
            SUP,
            action=["role_delete"],  # type: ignore[arg-type]
            resource_type="role",
            resource_id="reader",
            namespace="role",
            outcome="no_change",
            cause="resource_missing",
            action_context=next(_contexts()),
            conditions=[{"kind": "resource_absent"}],
        )
    with pytest.raises(ValueError, match="condition kind"):
        catalog.rbac_append_attempt(
            ORG,
            SUP,
            action="role_delete",
            resource_type="role",
            resource_id="reader",
            namespace="role",
            outcome="no_change",
            cause="resource_missing",
            action_context=next(_contexts()),
            conditions=[{"kind": []}],
        )

    assert catalog.r.xlen(RK.audit_privileged_outbox(ORG)) == 0


def test_false_resource_missing_claim_becomes_truthful_failure_evidence(
    catalog: RedisCatalog,
) -> None:
    contexts = _contexts()
    catalog.rbac_create_role(
        ORG, SUP, "reader", _role("reader"), action_context=next(contexts)
    )

    with pytest.raises(RbacAuditConditionConflict):
        catalog.rbac_append_attempt(
            ORG,
            SUP,
            action="role_delete",
            resource_type="role",
            resource_id="reader",
            namespace="role",
            outcome="no_change",
            cause="resource_missing",
            action_context=next(contexts),
            conditions=[{"kind": "resource_absent"}],
        )

    events = [
        entry.event
        for entry in _outbox(catalog).query(count=10, newest_first=False)
    ]
    assert [event["outcome"] for event in events] == ["success", "failure"]
    assert events[-1]["cause"] == "concurrent_modification"
    assert catalog.rbac_role_exists(ORG, SUP, "reader")


def test_user_idempotent_replay_rejects_corrupt_assignment_object(
    catalog: RedisCatalog,
) -> None:
    _, user_manager = _managers(catalog)
    catalog.rbac_create_user(
        ORG,
        SUP,
        "alice-id",
        {"user_id": "alice-id", "username": "alice", "roles": []},
        action_context=next(_contexts()),
    )
    catalog.r.hset(
        RK.rbac_user_doc(ORG, SUP, "alice-id"), "roles", "{}",
    )

    with pytest.raises(RbacIntegrityError):
        user_manager.create_user(
            {"username": "alice", "roles": []},
            action_context=next(_contexts()),
        )

    events = _outbox(catalog).query(count=10, newest_first=False)
    assert len(events) == 2
    assert events[0].event["outcome"] == "success"
    assert events[1].event["outcome"] == "failure"
    assert events[1].event["cause"] == "state_integrity_error"


def test_deprecated_bulk_role_removal_is_durably_denied_without_mutation(
    catalog: RedisCatalog,
) -> None:
    _, user_manager = _managers(catalog)
    audit_keys = {
        RK.audit_privileged_outbox(ORG),
        RK.audit_privileged_meta(ORG),
    }
    before = {
        key: catalog.r.dump(key)
        for key in catalog.r.keys("*")
        if key not in audit_keys
    }

    with pytest.raises(RbacDecisionError, match="unsupported"):
        user_manager.remove_role_from_users(
            "reader", action_context=next(_contexts()),
        )

    event = _outbox(catalog).query(count=1)[0].event
    assert event["outcome"] == "denied"
    assert event["cause"] == "unsupported_bulk_operation"
    # Only the audit stream/meta may change for a denied operation.
    after = {
        key: catalog.r.dump(key)
        for key in catalog.r.keys("*")
        if key not in audit_keys
    }
    assert after == before


def test_catalog_noops_are_ordered_no_change_records_without_state_mutation(
    catalog: RedisCatalog,
) -> None:
    contexts = _contexts()
    catalog.rbac_create_role(
        ORG, SUP, "reader", _role("reader"), action_context=next(contexts)
    )
    catalog.rbac_create_user(
        ORG,
        SUP,
        "alice-id",
        {"user_id": "alice-id", "username": "alice", "roles": ["reader"]},
        action_context=next(contexts),
    )
    user_before = catalog.r.hgetall(RK.rbac_user_doc(ORG, SUP, "alice-id"))
    role_meta_before = catalog.r.hgetall(RK.rbac_role_meta(ORG, SUP))
    user_meta_before = catalog.r.hgetall(RK.rbac_user_meta(ORG, SUP))

    assert not catalog.rbac_add_role_to_user(
        ORG, SUP, "alice-id", "reader", action_context=next(contexts)
    )
    assert not catalog.rbac_remove_role_from_user(
        ORG, SUP, "alice-id", "absent", action_context=next(contexts)
    )
    assert not catalog.rbac_delete_role(
        ORG, SUP, "absent", action_context=next(contexts)
    )

    assert catalog.r.hgetall(RK.rbac_user_doc(ORG, SUP, "alice-id")) == user_before
    assert catalog.r.hgetall(RK.rbac_role_meta(ORG, SUP)) == role_meta_before
    assert catalog.r.hgetall(RK.rbac_user_meta(ORG, SUP)) == user_meta_before
    entries = _outbox(catalog).query(count=10, newest_first=False)
    assert [entry.event["ledger_sequence"] for entry in entries] == [1, 2, 3, 4, 5]
    assert [entry.event["outcome"] for entry in entries[-3:]] == [
        "no_change", "no_change", "no_change",
    ]
    assert [entry.event["cause"] for entry in entries[-3:]] == [
        "role_already_assigned", "role_not_assigned", "resource_missing",
    ]


@pytest.mark.parametrize(
    "scenario",
    (
        "role_create_replay",
        "user_create_replay",
        "role_delete_missing",
        "user_delete_missing",
        "empty_user_update",
        "assign_user_missing",
        "assign_already",
        "remove_not_assigned",
    ),
)
def test_state_dependent_noop_race_never_appends_stale_evidence(
    catalog: RedisCatalog,
    scenario: str,
) -> None:
    contexts = _contexts()
    role_manager, user_manager = _managers(catalog)

    if scenario == "role_create_replay":
        role_data = {
            "role": "reader",
            "role_name": "reader",
            "tables": _role("reader")["tables"],
        }
        catalog.rbac_create_role(
            ORG, SUP, "reader-id", role_data,
            action_context=next(contexts),
        )
        operation = lambda: role_manager.create_role(
            role_data, action_context=next(contexts),
        )
        mutation = lambda: catalog.r.hincrby(
            RK.rbac_role_doc(ORG, SUP, "reader-id"), "doc_version", 1,
        )
    elif scenario == "user_create_replay":
        catalog.rbac_create_user(
            ORG,
            SUP,
            "alice-id",
            {"user_id": "alice-id", "username": "alice", "roles": []},
            action_context=next(contexts),
        )
        operation = lambda: user_manager.create_user(
            {"username": "alice", "roles": []},
            action_context=next(contexts),
        )
        mutation = lambda: catalog.r.hincrby(
            RK.rbac_user_doc(ORG, SUP, "alice-id"), "doc_version", 1,
        )
    elif scenario == "role_delete_missing":
        operation = lambda: catalog.rbac_delete_role(
            ORG, SUP, "missing-role", action_context=next(contexts),
        )
        mutation = lambda: catalog.r.hset(
            RK.rbac_role_doc(ORG, SUP, "missing-role"),
            mapping={"role_id": "missing-role", "doc_version": "1"},
        )
    elif scenario == "user_delete_missing":
        operation = lambda: catalog.rbac_delete_user(
            ORG, SUP, "missing-user", action_context=next(contexts),
        )
        mutation = lambda: catalog.r.hset(
            RK.rbac_user_doc(ORG, SUP, "missing-user"),
            mapping={"user_id": "missing-user", "doc_version": "1"},
        )
    elif scenario == "empty_user_update":
        catalog.rbac_create_user(
            ORG,
            SUP,
            "alice-id",
            {"user_id": "alice-id", "username": "alice", "roles": []},
            action_context=next(contexts),
        )
        operation = lambda: user_manager.modify_user(
            "alice-id", {}, action_context=next(contexts),
        )
        mutation = lambda: catalog.r.hincrby(
            RK.rbac_user_doc(ORG, SUP, "alice-id"), "doc_version", 1,
        )
    elif scenario == "assign_user_missing":
        operation = lambda: catalog.rbac_add_role_to_user(
            ORG, SUP, "missing-user", "reader",
            action_context=next(contexts),
        )
        mutation = lambda: catalog.r.hset(
            RK.rbac_user_doc(ORG, SUP, "missing-user"),
            mapping={"roles": "[]", "doc_version": "1"},
        )
    elif scenario in {"assign_already", "remove_not_assigned"}:
        catalog.rbac_create_role(
            ORG, SUP, "reader", _role("reader"),
            action_context=next(contexts),
        )
        assigned = ["reader"] if scenario == "assign_already" else []
        catalog.rbac_create_user(
            ORG,
            SUP,
            "alice-id",
            {"user_id": "alice-id", "username": "alice", "roles": assigned},
            action_context=next(contexts),
        )
        method = (
            catalog.rbac_add_role_to_user
            if scenario == "assign_already"
            else catalog.rbac_remove_role_from_user
        )
        operation = lambda: method(
            ORG, SUP, "alice-id", "reader", action_context=next(contexts),
        )
        mutation = lambda: catalog.r.hincrby(
            RK.rbac_user_doc(ORG, SUP, "alice-id"), "doc_version", 1,
        )
    else:
        operation = lambda: user_manager.remove_role_from_users(
            "reader", action_context=next(contexts),
        )
        mutation = lambda: catalog.r.sadd(
            RK.rbac_user_index(ORG, SUP), "appeared-user",
        )

    stream_length = catalog.r.xlen(RK.audit_privileged_outbox(ORG))
    ledger_sequence = catalog.r.hget(
        RK.audit_privileged_meta(ORG), "sequence",
    )
    _race_conditional_append(catalog, mutation)

    with pytest.raises(RbacAuditConditionConflict, match="changed concurrently"):
        operation()

    assert catalog.r.xlen(RK.audit_privileged_outbox(ORG)) == stream_length + 1
    assert int(catalog.r.hget(
        RK.audit_privileged_meta(ORG), "sequence",
    )) == int(ledger_sequence or "0") + 1
    conflict = _outbox(catalog).query(count=1)[0].event
    assert conflict["outcome"] == "failure"
    assert conflict["cause"] == "concurrent_modification"


@pytest.mark.parametrize("manager_kind", ("role", "user"))
def test_manager_read_backend_error_is_not_audited_as_resource_missing(
    catalog: RedisCatalog,
    manager_kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    role_manager, user_manager = _managers(catalog)
    target = RK.rbac_role_doc(ORG, SUP, "target")
    manager = role_manager
    operation = lambda: manager.delete_role("target")
    if manager_kind == "user":
        target = RK.rbac_user_doc(ORG, SUP, "target")
        manager = user_manager
        operation = lambda: manager.delete_user("target")
    original = catalog.r.hgetall

    def failing_read(key):
        if key == target:
            raise ResponseError("backend unavailable")
        return original(key)

    monkeypatch.setattr(catalog.r, "hgetall", failing_read)
    with pytest.raises(ResponseError, match="backend unavailable"):
        operation()
    assert catalog.r.xlen(RK.audit_privileged_outbox(ORG)) == 0


def test_manager_validation_rejections_record_once_without_catalog_mutation(
    catalog: RedisCatalog,
) -> None:
    role_manager = RoleManager.__new__(RoleManager)
    role_manager.organization = ORG
    role_manager.super_name = SUP
    role_manager._catalog = catalog
    user_manager = UserManager.__new__(UserManager)
    user_manager.organization = ORG
    user_manager.super_name = SUP
    user_manager._catalog = catalog

    with pytest.raises(ValueError, match="role data"):
        role_manager.create_role("not-an-object", action_context=next(_contexts()))
    with pytest.raises(ValueError, match="username is required"):
        user_manager.create_user({}, action_context=next(_contexts()))

    entries = _outbox(catalog).query(count=10, newest_first=False)
    assert len(entries) == 2
    assert [entry.event["action"] for entry in entries] == [
        "role_create", "user_create",
    ]
    assert [entry.event["outcome"] for entry in entries] == ["denied", "denied"]
    assert all(entry.event["cause"] == "request_rejected" for entry in entries)
    assert catalog.r.exists(RK.rbac_role_index(ORG, SUP)) == 0
    assert catalog.r.exists(RK.rbac_user_index(ORG, SUP)) == 0


def test_exception_message_cannot_spoof_a_no_change_outcome(
    catalog: RedisCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    role_manager, _ = _managers(catalog)

    def spoofed_message(*args, **kwargs):
        raise ValueError("Role missing does not exist and is already assigned")

    monkeypatch.setattr(role_manager, "_prepare_role_content", spoofed_message)
    with pytest.raises(ValueError, match="does not exist"):
        role_manager.create_role(
            {"role": "reader", "role_name": "spoof", "tables": {}},
            action_context=next(_contexts()),
        )

    event = _outbox(catalog).query(count=1)[0].event
    assert event["outcome"] == "denied"
    assert event["cause"] == "request_rejected"


def test_ambiguous_backend_exception_is_not_relabelled_as_rbac_failure(
    catalog: RedisCatalog,
) -> None:
    contexts = _contexts()
    catalog.rbac_create_role(
        ORG, SUP, "reader", _role("reader"), action_context=next(contexts)
    )

    def ambiguous_backend(*, keys: list[str], args: list[str]) -> Any:
        raise ResponseError("ambiguous transport result")

    catalog._rbac_update_role = ambiguous_backend
    with pytest.raises(ResponseError, match="ambiguous transport"):
        catalog.rbac_update_role(
            ORG,
            SUP,
            "reader",
            {"role_name": "must-not-be-labelled"},
            action_context=next(contexts),
        )

    entries = _outbox(catalog).query(count=10, newest_first=False)
    assert len(entries) == 1
    assert entries[0].event["outcome"] == "success"


def test_all_eight_mutations_commit_complete_ordered_records(
    catalog: RedisCatalog,
) -> None:
    contexts = _contexts()

    catalog.rbac_create_role(
        ORG, SUP, "reader", _role("reader"), action_context=next(contexts)
    )
    catalog.rbac_update_role(
        ORG,
        SUP,
        "reader",
        {"role_name": "reader-renamed"},
        action_context=next(contexts),
    )
    catalog.rbac_create_role(
        ORG,
        SUP,
        "writer",
        _role("writer", role_type="writer"),
        action_context=next(contexts),
    )
    catalog.rbac_create_user(
        ORG,
        SUP,
        "alice-id",
        {"user_id": "alice-id", "username": "alice", "roles": ["reader"]},
        action_context=next(contexts),
    )
    catalog.rbac_update_user(
        ORG,
        SUP,
        "alice-id",
        {"display_name": "Alice Example"},
        action_context=next(contexts),
    )
    assert catalog.rbac_add_role_to_user(
        ORG,
        SUP,
        "alice-id",
        "writer",
        action_context=next(contexts),
    )
    assert catalog.rbac_remove_role_from_user(
        ORG,
        SUP,
        "alice-id",
        "writer",
        action_context=next(contexts),
    )

    # Keep one assignment alive so role deletion must perform and report a
    # cascade.  The extra create is intentionally visible in the ledger.
    catalog.rbac_create_user(
        ORG,
        SUP,
        "cascade-id",
        {"user_id": "cascade-id", "username": "cascade", "roles": ["reader"]},
        action_context=next(contexts),
    )
    catalog.rbac_delete_user(
        ORG, SUP, "alice-id", action_context=next(contexts)
    )
    assert catalog.rbac_delete_role(
        ORG, SUP, "reader", action_context=next(contexts)
    )

    entries = _outbox(catalog).query(count=100, newest_first=False)
    actions = [entry.event["action"] for entry in entries]
    assert actions == [
        "role_create",
        "role_update",
        "role_create",
        "user_create",
        "user_update",
        "user_role_assign",
        "user_role_remove",
        "user_create",
        "user_delete",
        "role_delete",
    ]
    assert set(actions) == {
        "role_create",
        "role_update",
        "role_delete",
        "user_create",
        "user_update",
        "user_delete",
        "user_role_assign",
        "user_role_remove",
    }

    # Both the Redis stream envelope and the shared validator expose an exact,
    # gap-free commit order.  Reading the committed JSON again proves the
    # outbox did not merely accept an unchecked mapping.
    assert [entry.event["ledger_sequence"] for entry in entries] == list(
        range(1, 11)
    )
    assert [
        fields["ledger_sequence"]
        for _, fields in catalog.r.xrange(RK.audit_privileged_outbox(ORG))
    ] == [str(number) for number in range(1, 11)]
    assert catalog.r.hget(RK.audit_privileged_meta(ORG), "sequence") == "10"
    for sequence, entry in enumerate(entries, start=1):
        # The producer stores the validated pre-commit template byte-for-byte;
        # commit-assigned counters live in separate exact-decimal fields.
        template = PrivilegedAuditRecord.from_json(entry.event_json)
        assert entry.event_json == template.to_json()
        assert template.ledger_sequence == 0
        assert template.namespace_version == 0
        assert template.affected_count == 0
        record = PrivilegedAuditRecord.from_json(entry.committed_event_json)
        assert record.to_dict() == dict(entry.event)
        assert record.ledger_sequence == sequence
        assert record.actor_type == "user"
        assert record.actor_id == "security-admin-id"
        assert record.actor_username == "security.admin"
        assert record.actor_ip == "192.0.2.10"
        assert record.actor_user_agent == "rbac-integration-test"
        assert record.correlation_id == "correlation-42"
        assert record.session_id == "session-7"
        assert record.server == "control-plane-1"
        assert record.reason == "approved access maintenance"
        assert record.ticket_id == "SEC-4242"
        assert record.mutation_id == f"mutation-{sequence}"
        assert record.cause == "admin_api"
        assert not record.context_missing

    by_sequence = {
        entry.event["ledger_sequence"]: entry.event for entry in entries
    }
    assert (by_sequence[1]["before_version"], by_sequence[1]["after_version"]) == (
        0,
        1,
    )
    assert by_sequence[1]["before_sha256"] == ""
    assert by_sequence[1]["after_sha256"]
    assert (by_sequence[2]["before_version"], by_sequence[2]["after_version"]) == (
        1,
        2,
    )
    assert by_sequence[2]["before_sha256"]
    assert by_sequence[2]["after_sha256"]
    assert by_sequence[2]["before_sha256"] != by_sequence[2]["after_sha256"]

    assert by_sequence[4]["role_ids_added"] == ["reader"]
    assert by_sequence[4]["role_ids_removed"] == []
    assert (by_sequence[5]["before_version"], by_sequence[5]["after_version"]) == (
        1,
        2,
    )
    assert by_sequence[6]["role_ids_added"] == ["writer"]
    assert by_sequence[6]["role_ids_removed"] == []
    assert (by_sequence[6]["before_version"], by_sequence[6]["after_version"]) == (
        2,
        3,
    )
    assert by_sequence[7]["role_ids_added"] == []
    assert by_sequence[7]["role_ids_removed"] == ["writer"]
    assert (by_sequence[7]["before_version"], by_sequence[7]["after_version"]) == (
        3,
        4,
    )
    assert by_sequence[9]["role_ids_removed"] == ["reader"]
    assert (by_sequence[9]["before_version"], by_sequence[9]["after_version"]) == (
        4,
        0,
    )
    assert by_sequence[9]["before_sha256"]
    assert by_sequence[9]["after_sha256"] == ""

    deletion = by_sequence[10]
    assert deletion["affected_count"] == 1
    assert deletion["cascade_assignment_count"] == 1
    assert deletion["cascade_manifest_id"] == deletion["event_id"]
    assert deletion["user_namespace_version_after"] == (
        deletion["user_namespace_version_before"] + 1
    )
    assert deletion["namespace_version"] == 4
    assert (deletion["before_version"], deletion["after_version"]) == (2, 0)
    assert "user.roles" in deletion["changed_fields"]
    assert deletion["before_sha256"]
    assert deletion["after_sha256"] == ""
    manifest = _outbox(catalog).get_cascade_manifest(entries[-1])
    assert manifest is not None
    assert manifest.event_id == deletion["event_id"]
    assert manifest.role_id == "reader"
    assert manifest.user_count == 1
    assert manifest.removed_assignment_count == 1
    assert [row.user_id for row in manifest.rows] == ["cascade-id"]
    assert manifest.rows[0].before_doc_version == 1
    assert manifest.rows[0].after_doc_version == 2
    assert manifest.rows[0].removed_occurrences == 1
    assert manifest.rows[0].before_role_count == 1
    assert manifest.rows[0].after_role_count == 0

    # Redis cjson encodes an empty Lua table as {}, so the production script
    # must deliberately persist the last-role result as a JSON array.
    stored_roles = catalog.r.hget(
        RK.rbac_user_doc(ORG, SUP, "cascade-id"), "roles"
    )
    assert stored_roles == "[]"
    assert json.loads(stored_roles) == []
    assert catalog.r.hget(
        RK.rbac_user_doc(ORG, SUP, "cascade-id"), "doc_version"
    ) == "2"


def test_role_delete_cascade_records_exact_users_and_duplicate_occurrences(
    catalog: RedisCatalog,
) -> None:
    contexts = _contexts()
    catalog.rbac_create_role(
        ORG, SUP, "duplicated", _role("duplicated"),
        action_context=next(contexts),
    )
    catalog.rbac_create_user(
        ORG,
        SUP,
        "duplicate-user",
        {
            "user_id": "duplicate-user",
            "username": "duplicate-user",
            "roles": ["duplicated", "duplicated"],
        },
        action_context=next(contexts),
    )

    assert catalog.rbac_delete_role(
        ORG, SUP, "duplicated", action_context=next(contexts),
    )

    deletion = _outbox(catalog).query(count=1)[0]
    assert deletion.event["affected_count"] == 1
    assert deletion.event["cascade_assignment_count"] == 2
    manifest = _outbox(catalog).get_cascade_manifest(deletion)
    assert manifest is not None
    assert manifest.user_count == 1
    assert manifest.removed_assignment_count == 2
    assert manifest.rows == (
        manifest.rows[0],
    )
    row = manifest.rows[0]
    assert row.user_id == "duplicate-user"
    assert row.removed_occurrences == 2
    assert (row.before_role_count, row.after_role_count) == (2, 0)
    assert catalog.r.hget(
        RK.rbac_user_doc(ORG, SUP, "duplicate-user"), "roles"
    ) == "[]"
    raw_manifest = catalog.r.hgetall(
        RK.audit_privileged_cascade(ORG, deletion.event["event_id"])
    )
    assert set(raw_manifest) == {
        "schema_version",
        "event_id",
        "mutation_id",
        "organization",
        "super_name",
        "role_id",
        "user_count",
        "removed_assignment_count",
        "user_namespace_version_before",
        "user_namespace_version_after",
        "created_ms",
        "user:duplicate-user",
    }
    assert not any(
        forbidden in json.dumps(raw_manifest).casefold()
        for forbidden in ("tables", "filters", "username", "account_id")
    )


def test_role_delete_rejects_json_object_roles_without_manifest_or_mutation(
    catalog: RedisCatalog,
) -> None:
    contexts = _contexts()
    catalog.rbac_create_role(
        ORG, SUP, "corrupt-target", _role("corrupt-target"),
        action_context=next(contexts),
    )
    catalog.rbac_create_user(
        ORG,
        SUP,
        "corrupt-user",
        {
            "user_id": "corrupt-user",
            "username": "corrupt-user",
            "roles": ["corrupt-target"],
        },
        action_context=next(contexts),
    )
    catalog.r.hset(
        RK.rbac_user_doc(ORG, SUP, "corrupt-user"), "roles", "{}",
    )
    role_before = catalog.r.hgetall(
        RK.rbac_role_doc(ORG, SUP, "corrupt-target")
    )
    user_before = catalog.r.hgetall(
        RK.rbac_user_doc(ORG, SUP, "corrupt-user")
    )

    with pytest.raises(RbacIntegrityError, match="assignments are corrupt"):
        catalog.rbac_delete_role(
            ORG, SUP, "corrupt-target", action_context=next(contexts),
        )

    assert catalog.r.hgetall(
        RK.rbac_role_doc(ORG, SUP, "corrupt-target")
    ) == role_before
    assert catalog.r.hgetall(
        RK.rbac_user_doc(ORG, SUP, "corrupt-user")
    ) == user_before
    assert not any(
        ":audit:privileged:cascade:doc:" in key
        for key in catalog.r.scan_iter()
    )
    assert _outbox(catalog).query(count=1)[0].event["outcome"] == "failure"


def test_role_delete_rejects_orphaned_user_identity_sources_before_mutation(
    catalog: RedisCatalog,
) -> None:
    contexts = _contexts()
    catalog.rbac_create_role(
        ORG, SUP, "target", _role("target"), action_context=next(contexts)
    )
    catalog.rbac_create_user(
        ORG,
        SUP,
        "alice-id",
        {"username": "alice", "roles": ["target"]},
        action_context=next(contexts),
    )
    role_key = RK.rbac_role_doc(ORG, SUP, "target")
    before = catalog.r.hgetall(role_key)
    catalog.r.srem(RK.rbac_user_index(ORG, SUP), "alice-id")

    with pytest.raises(RbacIntegrityError, match="inconsistent"):
        catalog.rbac_delete_role(
            ORG, SUP, "target", action_context=next(contexts),
        )

    assert catalog.r.hgetall(role_key) == before
    assert "target" in catalog.get_user_details(
        ORG, SUP, "alice-id",
    )["roles"]
    event = _outbox(catalog).query(count=1)[0].event
    assert event["outcome"] == "failure"
    assert event["cause"] == "state_integrity_error"


def test_role_delete_fails_before_writes_at_configured_cascade_cap_plus_one(
    catalog: RedisCatalog,
) -> None:
    contexts = _contexts()
    catalog._RBAC_CASCADE_MANIFEST_USER_LIMIT = 2
    catalog.rbac_create_role(
        ORG, SUP, "wide-role", _role("wide-role"),
        action_context=next(contexts),
    )
    for number in range(3):
        user_id = f"wide-user-{number}"
        catalog.rbac_create_user(
            ORG,
            SUP,
            user_id,
            {
                "user_id": user_id,
                "username": user_id,
                # No assignment to the target: the bound protects total scan
                # work, not merely the size of the resulting manifest.
                "roles": [],
            },
            action_context=next(contexts),
        )
    role_before = catalog.r.hgetall(RK.rbac_role_doc(ORG, SUP, "wide-role"))
    users_before = {
        user_id: catalog.r.hgetall(RK.rbac_user_doc(ORG, SUP, user_id))
        for user_id in ("wide-user-0", "wide-user-1", "wide-user-2")
    }

    with pytest.raises(ValueError, match="atomic audit limit"):
        catalog.rbac_delete_role(
            ORG, SUP, "wide-role", action_context=next(contexts),
        )

    assert catalog.r.hgetall(
        RK.rbac_role_doc(ORG, SUP, "wide-role")
    ) == role_before
    assert {
        user_id: catalog.r.hgetall(RK.rbac_user_doc(ORG, SUP, user_id))
        for user_id in users_before
    } == users_before
    assert not any(
        ":audit:privileged:cascade:doc:" in key
        for key in catalog.r.scan_iter()
    )


def test_nonempty_privileged_stream_with_zero_sequence_head_fails_closed(
    catalog: RedisCatalog,
) -> None:
    contexts = _contexts()
    catalog.rbac_create_role(
        ORG, SUP, "first-role", _role("first-role"),
        action_context=next(contexts),
    )
    stream_key = RK.audit_privileged_outbox(ORG)
    meta_key = RK.audit_privileged_meta(ORG)
    original_id, original_fields = catalog.r.xrange(stream_key)[0]
    catalog.r.xdel(stream_key, original_id)
    corrupt_fields = dict(original_fields)
    corrupt_fields["ledger_sequence"] = "0"
    corrupt_id = catalog.r.xadd(stream_key, corrupt_fields)
    catalog.r.hset(meta_key, mapping={
        "sequence": "0",
        "last_stream_id": corrupt_id,
        "last_event_id": corrupt_fields["event_id"],
        "last_payload_hash": corrupt_fields["payload_hash"],
    })

    with pytest.raises(ResponseError, match="zero sequence head"):
        catalog.rbac_create_role(
            ORG, SUP, "must-not-commit", _role("must-not-commit"),
            action_context=next(contexts),
        )

    assert not catalog.r.exists(
        RK.rbac_role_doc(ORG, SUP, "must-not-commit")
    )
    assert catalog.r.xlen(stream_key) == 1


@pytest.mark.parametrize("meta_field", ["last_event_id", "last_payload_hash"])
def test_privileged_stream_head_identity_must_match_meta(
    catalog: RedisCatalog,
    meta_field: str,
) -> None:
    contexts = _contexts()
    catalog.rbac_create_role(
        ORG, SUP, "first-role", _role("first-role"),
        action_context=next(contexts),
    )
    catalog.r.hset(RK.audit_privileged_meta(ORG), meta_field, "tampered")

    with pytest.raises(ResponseError, match="heads disagree"):
        catalog.rbac_create_role(
            ORG, SUP, "must-not-commit", _role("must-not-commit"),
            action_context=next(contexts),
        )

    assert not catalog.r.exists(
        RK.rbac_role_doc(ORG, SUP, "must-not-commit")
    )


def test_removing_a_users_last_role_persists_an_empty_json_array(
    catalog: RedisCatalog,
) -> None:
    contexts = _contexts()
    catalog.rbac_create_role(
        ORG, SUP, "reader", _role("reader"), action_context=next(contexts)
    )
    catalog.rbac_create_user(
        ORG,
        SUP,
        "alice-id",
        {"user_id": "alice-id", "username": "alice", "roles": ["reader"]},
        action_context=next(contexts),
    )

    assert catalog.rbac_remove_role_from_user(
        ORG,
        SUP,
        "alice-id",
        "reader",
        action_context=next(contexts),
    )

    stored_roles = catalog.r.hget(
        RK.rbac_user_doc(ORG, SUP, "alice-id"), "roles"
    )
    assert stored_roles == "[]"
    assert json.loads(stored_roles) == []
    entry = _outbox(catalog).query(count=1)[0]
    assert entry.event["action"] == "user_role_remove"
    assert entry.event["role_ids_removed"] == ["reader"]
    assert entry.event["ledger_sequence"] == 3


def test_private_lua_noop_skips_success_but_catalog_cas_appends_failure(
    catalog: RedisCatalog,
) -> None:
    contexts = _contexts()
    catalog.rbac_create_role(
        ORG, SUP, "reader", _role("reader"), action_context=next(contexts)
    )
    catalog.rbac_create_user(
        ORG,
        SUP,
        "alice-id",
        {"user_id": "alice-id", "username": "alice", "roles": ["reader"]},
        action_context=next(contexts),
    )
    stream = RK.audit_privileged_outbox(ORG)
    assert catalog.r.xlen(stream) == 2

    # Invoke the registered production script directly.  The role is already
    # present, so its Lua no-op branch must run before append_privileged_audit.
    user_key = RK.rbac_user_doc(ORG, SUP, "alice-id")
    raw = catalog.r.hgetall(user_key)
    expected_roles = raw["roles"]
    expected_version = raw["doc_version"]
    noop_json = catalog._rbac_audit_json(
        action_context=next(contexts),
        org=ORG,
        sup=SUP,
        action="user_role_assign",
        resource_type="user_role_assignment",
        resource_id="alice-id:reader",
        before_document=raw,
        after_document=raw,
        before_version=int(expected_version),
        after_version=int(expected_version) + 1,
        role_ids_added=("reader",),
    )
    result = catalog._rbac_add_role_to_user(
        keys=[
            user_key,
            RK.rbac_user_meta(ORG, SUP),
            RK.rbac_role_doc(ORG, SUP, "reader"),
        ]
        + catalog._rbac_audit_keys(ORG),
        args=[
            "reader",
            "1",
            "alice-id",
            expected_roles,
            expected_version,
            ORG,
            SUP,
            noop_json,
        ],
    )
    assert int(result) == 0
    assert catalog.r.xlen(stream) == 2

    # Race a high-level update after it has read its expected snapshot.  The
    # wrapper injects the concurrent write and still calls the real Lua script.
    registered_update = catalog._rbac_update_role

    def race_then_execute(*, keys: list[str], args: list[str]) -> Any:
        catalog.r.hset(keys[0], "modified_ms", "concurrent-writer")
        return registered_update(keys=keys, args=args)

    catalog._rbac_update_role = race_then_execute
    with pytest.raises(ValueError, match="changed concurrently"):
        catalog.rbac_update_role(
            ORG,
            SUP,
            "reader",
            {"role_name": "must-not-commit"},
            action_context=next(contexts),
        )

    assert catalog.r.xlen(stream) == 3
    failed = _outbox(catalog).query(count=1)[0].event
    assert failed["outcome"] == "failure"
    assert failed["cause"] == "concurrent_modification"
    assert failed["ledger_sequence"] == 3
    assert catalog.r.hget(
        RK.rbac_role_doc(ORG, SUP, "reader"), "role_name"
    ) == "reader"
    assert catalog.r.hget(RK.rbac_role_meta(ORG, SUP), "version") == "1"


@pytest.mark.parametrize("poisoned_key", ["outbox", "meta"])
def test_wrong_type_audit_key_aborts_before_rbac_state_changes(
    catalog: RedisCatalog,
    poisoned_key: str,
) -> None:
    contexts = _contexts()
    catalog.rbac_create_role(
        ORG, SUP, "reader", _role("reader"), action_context=next(contexts)
    )
    before = _rbac_role_state(catalog, "reader")
    outbox_key = RK.audit_privileged_outbox(ORG)
    audit_meta_key = RK.audit_privileged_meta(ORG)

    if poisoned_key == "outbox":
        catalog.r.delete(outbox_key)
        catalog.r.set(outbox_key, "wrong-type")
        previous_stream_length = None
    else:
        catalog.r.delete(audit_meta_key)
        catalog.r.set(audit_meta_key, "wrong-type")
        previous_stream_length = catalog.r.xlen(outbox_key)

    with pytest.raises(ResponseError, match="wrong Redis type"):
        catalog.rbac_update_role(
            ORG,
            SUP,
            "reader",
            {"role_name": "must-not-commit"},
            action_context=next(contexts),
        )

    assert _rbac_role_state(catalog, "reader") == before
    if poisoned_key == "outbox":
        assert catalog.r.get(outbox_key) == "wrong-type"
        assert catalog.r.hget(audit_meta_key, "sequence") == "1"
    else:
        assert catalog.r.get(audit_meta_key) == "wrong-type"
        assert catalog.r.xlen(outbox_key) == previous_stream_length == 1


def test_sequence_is_exact_above_binary64_integer_precision(
    catalog: RedisCatalog,
) -> None:
    start = 2**53
    contexts = _contexts()

    # Establish a genuine stream/meta head, then move that exact envelope to
    # the high starting sequence.  Setting meta alone is intentionally invalid:
    # the producer must reject any stream/meta head disagreement.
    catalog.rbac_create_role(
        ORG, SUP, "seed", _role("seed"), action_context=next(contexts)
    )
    stream = RK.audit_privileged_outbox(ORG)
    seed_id, seed_fields = catalog.r.xrevrange(stream, count=1)[0]
    anchored_fields = dict(seed_fields)
    anchored_fields["ledger_sequence"] = str(start)
    anchored_id = catalog.r.xadd(stream, anchored_fields)
    catalog.r.xdel(stream, seed_id)
    catalog.r.hset(
        RK.audit_privileged_meta(ORG),
        mapping={"sequence": str(start), "last_stream_id": anchored_id},
    )

    catalog.rbac_create_role(
        ORG, SUP, "reader", _role("reader"), action_context=next(contexts)
    )
    catalog.rbac_create_role(
        ORG,
        SUP,
        "writer",
        _role("writer", role_type="writer"),
        action_context=next(contexts),
    )

    raw_entries = catalog.r.xrange(stream)
    assert [fields["ledger_sequence"] for _, fields in raw_entries] == [
        str(start),
        str(start + 1),
        str(start + 2),
    ]
    assert catalog.r.hget(RK.audit_privileged_meta(ORG), "sequence") == str(
        start + 2
    )
    assert [
        entry.event["ledger_sequence"]
        for entry in _outbox(catalog).query(newest_first=False)
    ] == [start, start + 1, start + 2]


@pytest.mark.parametrize("missing", ["meta_key", "sequence_field"])
def test_nonempty_outbox_with_missing_sequence_head_never_mutates(
    catalog: RedisCatalog,
    missing: str,
) -> None:
    contexts = _contexts()
    catalog.rbac_create_role(
        ORG, SUP, "reader", _role("reader"), action_context=next(contexts)
    )
    stream = RK.audit_privileged_outbox(ORG)
    audit_meta = RK.audit_privileged_meta(ORG)
    before = _rbac_role_state(catalog, "reader")
    if missing == "meta_key":
        catalog.r.delete(audit_meta)
    else:
        catalog.r.hdel(audit_meta, "sequence")

    with pytest.raises(ResponseError, match="sequence head is missing"):
        catalog.rbac_update_role(
            ORG,
            SUP,
            "reader",
            {"role_name": "must-not-commit"},
            action_context=next(contexts),
        )

    assert _rbac_role_state(catalog, "reader") == before
    assert catalog.r.xlen(stream) == 1
    assert catalog.r.hget(audit_meta, "sequence") is None


def test_positive_sequence_with_empty_stream_never_mutates(
    catalog: RedisCatalog,
) -> None:
    contexts = _contexts()
    catalog.rbac_create_role(
        ORG, SUP, "reader", _role("reader"), action_context=next(contexts)
    )
    stream = RK.audit_privileged_outbox(ORG)
    before = _rbac_role_state(catalog, "reader")
    existing_ids = [stream_id for stream_id, _ in catalog.r.xrange(stream)]
    assert len(existing_ids) == 1
    assert catalog.r.xdel(stream, *existing_ids) == 1
    assert catalog.r.xlen(stream) == 0
    assert catalog.r.hget(RK.audit_privileged_meta(ORG), "sequence") == "1"

    with pytest.raises(ResponseError, match="stream head is missing"):
        catalog.rbac_update_role(
            ORG,
            SUP,
            "reader",
            {"role_name": "must-not-commit"},
            action_context=next(contexts),
        )

    assert _rbac_role_state(catalog, "reader") == before
    assert catalog.r.xlen(stream) == 0


@pytest.mark.parametrize(
    ("head_field", "bad_value"),
    [("last_stream_id", "0-0"), ("sequence", "2")],
)
def test_stream_meta_head_mismatch_never_appends_or_mutates(
    catalog: RedisCatalog,
    head_field: str,
    bad_value: str,
) -> None:
    contexts = _contexts()
    catalog.rbac_create_role(
        ORG, SUP, "reader", _role("reader"), action_context=next(contexts)
    )
    stream = RK.audit_privileged_outbox(ORG)
    before = _rbac_role_state(catalog, "reader")
    catalog.r.hset(RK.audit_privileged_meta(ORG), head_field, bad_value)

    with pytest.raises(ResponseError, match="stream/meta heads disagree"):
        catalog.rbac_update_role(
            ORG,
            SUP,
            "reader",
            {"role_name": "must-not-commit"},
            action_context=next(contexts),
        )

    assert _rbac_role_state(catalog, "reader") == before
    assert catalog.r.xlen(stream) == 1


@pytest.mark.parametrize("counter", ["document", "namespace", "ledger"])
def test_noncanonical_leading_zero_counter_never_appends_or_mutates(
    catalog: RedisCatalog,
    counter: str,
) -> None:
    contexts = _contexts()
    catalog.rbac_create_role(
        ORG, SUP, "reader", _role("reader"), action_context=next(contexts)
    )
    stream = RK.audit_privileged_outbox(ORG)
    if counter == "document":
        catalog.r.hset(RK.rbac_role_doc(ORG, SUP, "reader"), "doc_version", "01")
    elif counter == "namespace":
        catalog.r.hset(RK.rbac_role_meta(ORG, SUP), "version", "01")
    else:
        catalog.r.hset(RK.audit_privileged_meta(ORG), "sequence", "01")
    before = _rbac_role_state(catalog, "reader")

    expected = ResponseError if counter == "ledger" else RbacIntegrityError
    with pytest.raises(expected):
        catalog.rbac_update_role(
            ORG,
            SUP,
            "reader",
            {"role_name": "must-not-commit"},
            action_context=next(contexts),
        )

    assert _rbac_role_state(catalog, "reader") == before
    assert catalog.r.xlen(stream) == (1 if counter == "ledger" else 2)


def test_delayed_meta_initialization_never_resets_committed_versions(
    catalog: RedisCatalog,
) -> None:
    contexts = _contexts()

    # Both commits intentionally happen before their usual bootstrap/meta
    # initializer, reproducing a delayed process that observed missing keys.
    catalog.rbac_create_role(
        ORG, SUP, "reader", _role("reader"), action_context=next(contexts)
    )
    catalog.rbac_create_user(
        ORG,
        SUP,
        "alice-id",
        {"user_id": "alice-id", "username": "alice", "roles": ["reader"]},
        action_context=next(contexts),
    )
    role_meta = RK.rbac_role_meta(ORG, SUP)
    user_meta = RK.rbac_user_meta(ORG, SUP)
    assert catalog.r.hget(role_meta, "version") == "1"
    assert catalog.r.hget(user_meta, "version") == "1"

    catalog.rbac_init_role_meta(ORG, SUP)
    catalog.rbac_init_user_meta(ORG, SUP)

    assert catalog.r.hget(role_meta, "version") == "1"
    assert catalog.r.hget(user_meta, "version") == "1"
    assert catalog.r.hget(role_meta, "initialized") == "true"
    assert catalog.r.hget(user_meta, "initialized") == "true"
    assert catalog.r.hget(RK.audit_privileged_meta(ORG), "sequence") == "2"


def test_empty_namespace_init_is_validation_only_until_first_audited_commit(
    catalog: RedisCatalog,
) -> None:
    role_meta = RK.rbac_role_meta(ORG, SUP)
    user_meta = RK.rbac_user_meta(ORG, SUP)

    catalog.rbac_init_role_meta(ORG, SUP)
    catalog.rbac_init_user_meta(ORG, SUP)

    assert catalog.r.exists(role_meta) == 0
    assert catalog.r.exists(user_meta) == 0
    assert catalog.r.exists(RK.rbac_role_index(ORG, SUP)) == 0
    assert catalog.r.exists(RK.rbac_user_index(ORG, SUP)) == 0
    assert catalog.r.xlen(RK.audit_privileged_outbox(ORG)) == 0
    assert not hasattr(catalog, "_rbac_bump_meta")
    assert not hasattr(RedisCatalog, "_LUA_RBAC_BUMP_META")

    catalog.rbac_create_role(
        ORG,
        SUP,
        "reader",
        _role("reader"),
        action_context=next(_contexts()),
    )

    assert catalog.r.hget(role_meta, "version") == "1"
    assert catalog.r.hget(role_meta, "initialized") == "true"
    event = _outbox(catalog).query(count=1)[0].event
    assert event["action"] == "role_create"
    assert event["outcome"] == "success"
    assert event["namespace_version"] == 1


def test_missing_namespace_head_with_live_state_fails_closed_and_is_audited(
    catalog: RedisCatalog,
) -> None:
    contexts = _contexts()
    catalog.rbac_create_role(
        ORG, SUP, "reader", _role("reader"), action_context=next(contexts)
    )
    role_key = RK.rbac_role_doc(ORG, SUP, "reader")
    before = catalog.r.hgetall(role_key)
    catalog.r.delete(RK.rbac_role_meta(ORG, SUP))

    with pytest.raises(RbacIntegrityError, match="integrity preflight"):
        catalog.rbac_update_role(
            ORG,
            SUP,
            "reader",
            {"role_name": "must-not-commit"},
            action_context=next(contexts),
        )

    assert catalog.r.hgetall(role_key) == before
    event = _outbox(catalog).query(count=1)[0].event
    assert event["outcome"] == "failure"
    assert event["cause"] == "state_integrity_error"
    assert event["namespace_version"] == 0
    with pytest.raises(RbacIntegrityError, match="safely initialized"):
        catalog.rbac_init_role_meta(ORG, SUP)
