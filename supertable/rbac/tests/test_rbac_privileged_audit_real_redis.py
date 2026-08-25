"""Opt-in real-Redis gate for the privileged RBAC Lua boundary.

Set ``SUPERTABLE_TEST_REDIS_URL`` to a disposable standalone Redis database.
The default suite skips this test because it must never target a shared or
production Redis instance.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import uuid

import pytest
import redis

from supertable import redis_keys as RK
from supertable.audit.privileged import PrivilegedActionContext
from supertable.audit.privileged_outbox import PrivilegedAuditOutbox
from supertable.audit.privileged_worker import (
    attest_activation_baseline,
    compute_privileged_state_sha256,
    verify_activation_baseline,
)
from supertable.redis_catalog import RedisCatalog


_SCRIPT_ATTRIBUTES = (
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
)


def _catalog(client) -> RedisCatalog:
    catalog = RedisCatalog.__new__(RedisCatalog)
    catalog.r = client
    for attribute in _SCRIPT_ATTRIBUTES:
        source = getattr(catalog, "_LUA" + attribute.upper())
        setattr(catalog, attribute, client.register_script(source))
    return catalog


def _context(index: int) -> PrivilegedActionContext:
    return PrivilegedActionContext(
        actor_type="system",
        actor_id="real-redis-audit-gate",
        correlation_id="real-redis-correlation",
        mutation_id=f"real-redis-mutation-{index}",
        reason="integration verification",
        ticket_id="AUDIT-E2E",
    )


def _install_activation_baseline(
    outbox: PrivilegedAuditOutbox,
    organization: str,
    baseline_path: Path,
) -> None:
    """Anchor the empty real-Redis estate through the production verifier."""
    document = {
        "version": 1,
        "kind": "supertable_privileged_activation_baseline",
        "organization": organization,
        "activation_id": f"real-redis-audit-gate-{organization}",
        "created_ms": 1_700_000_000_000,
        "state_sha256": compute_privileged_state_sha256(outbox, organization),
    }
    payload = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    baseline_path.write_bytes(payload)
    report = verify_activation_baseline(
        str(baseline_path),
        expected_sha256=hashlib.sha256(payload).hexdigest(),
        organization=organization,
    )
    assert attest_activation_baseline(outbox, report)


@pytest.mark.integration
def test_all_privileged_mutations_roundtrip_through_real_redis_lua(tmp_path):
    url = os.environ.get("SUPERTABLE_TEST_REDIS_URL", "").strip()
    if not url:
        pytest.skip("SUPERTABLE_TEST_REDIS_URL is not configured")

    client = redis.Redis.from_url(url, decode_responses=True)
    client.ping()
    token = uuid.uuid4().hex
    organization = f"audit-e2e-{token}"
    super_name = "lake"
    role_id = f"role-{token}"
    user_id = f"user-{token}"
    catalog = _catalog(client)
    try:
        outbox = PrivilegedAuditOutbox(
            client,
            stream_key=RK.audit_privileged_outbox(organization),
            delivery_ledger_key=RK.audit_privileged_delivery(organization),
        )
        _install_activation_baseline(
            outbox,
            organization,
            tmp_path / "privileged-activation-baseline.json",
        )
        role = {
            "role_id": role_id,
            "role": "reader",
            "role_name": f"reader-{token}",
            "tables": {"*": {"columns": ["*"], "filters": ["*"]}},
        }
        catalog.rbac_create_role(
            organization, super_name, role_id, role,
            action_context=_context(1),
        )
        catalog.rbac_update_role(
            organization, super_name, role_id,
            {"tables": {"orders": {"columns": ["id"], "filters": ["*"]}}},
            action_context=_context(2),
        )
        catalog.rbac_create_user(
            organization, super_name, user_id,
            {"user_id": user_id, "username": f"alice-{token}", "roles": []},
            action_context=_context(3),
        )
        catalog.rbac_update_user(
            organization, super_name, user_id,
            {"display_name": "Alice"},
            action_context=_context(4),
        )
        assert catalog.rbac_add_role_to_user(
            organization, super_name, user_id, role_id,
            action_context=_context(5),
        )
        assert catalog.rbac_remove_role_from_user(
            organization, super_name, user_id, role_id,
            action_context=_context(6),
        )
        assert catalog.rbac_add_role_to_user(
            organization, super_name, user_id, role_id,
            action_context=_context(7),
        )
        assert catalog.rbac_delete_role(
            organization, super_name, role_id,
            action_context=_context(8),
        )
        catalog.rbac_delete_user(
            organization, super_name, user_id,
            action_context=_context(9),
        )

        entries = outbox.query(newest_first=False)
        assert [entry.event["ledger_sequence"] for entry in entries] == list(
            range(1, 10)
        )
        assert [entry.event["action"] for entry in entries] == [
            "role_create",
            "role_update",
            "user_create",
            "user_update",
            "user_role_assign",
            "user_role_remove",
            "user_role_assign",
            "role_delete",
            "user_delete",
        ]
        cascade = outbox.get_cascade_manifest(entries[7])
        assert cascade is not None
        assert cascade.user_count == 1
        assert cascade.removed_assignment_count == 1
        assert cascade.rows[0].user_id == user_id
        assert entries[7].event["affected_count"] == 1
        assert entries[7].event["cascade_assignment_count"] == 1
        assert all(entry.event_json for entry in entries)
        assert all(entry.committed_event_json for entry in entries)
    finally:
        keys = list(client.scan_iter(match=RK.system_scope_pattern(organization)))
        keys.extend(client.scan_iter(match=RK.lakes_pattern(organization)))
        if keys:
            client.delete(*keys)
