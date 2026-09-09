"""Automatic genesis must work from empty Redis and preserve existing estates."""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from types import SimpleNamespace

import fakeredis
import pytest

from supertable import redis_keys as RK
import supertable.audit.bootstrap as bootstrap
from supertable.audit.bootstrap import ensure_greenfield_activation, read_activation_baseline
from supertable.audit.privileged_worker import (
    ActivationBaselineError,
    attest_activation_baseline,
    compute_privileged_state_sha256,
    verify_activation_baseline,
)


def _outbox(client, org="org"):
    return SimpleNamespace(_redis=client, stream_key=RK.audit_privileged_outbox(org))


def _snapshot(client):
    return {key: client.dump(key) for key in client.scan_iter()}


@pytest.mark.parametrize("decode_responses", [False, True])
def test_empty_organization_gets_a_worker_compatible_immutable_baseline(
    tmp_path, decode_responses,
):
    client = fakeredis.FakeStrictRedis(decode_responses=decode_responses)
    expected_state = compute_privileged_state_sha256(_outbox(client), "org")

    assert ensure_greenfield_activation(client, "org") is True
    original = client.get(RK.audit_privileged_activation("org"))
    anchor = json.loads(original)
    baseline = read_activation_baseline(client, "org")
    path = tmp_path / "baseline.json"
    path.write_bytes(baseline)

    assert anchor["state_sha256"] == expected_state
    assert anchor["artifact_sha256"] == hashlib.sha256(baseline).hexdigest()
    report = verify_activation_baseline(
        str(path), expected_sha256=anchor["artifact_sha256"], organization="org",
    )
    assert attest_activation_baseline(_outbox(client), report) is False
    # The verifier must continue to accept genesis after privileged state evolves.
    client.hset(RK.rbac_role_doc("org", "lake", "reader"), "doc_version", "1")
    assert ensure_greenfield_activation(client, "org") is False
    assert attest_activation_baseline(_outbox(client), report) is False
    assert client.get(RK.audit_privileged_activation("org")) == original


def test_concurrent_first_creators_share_one_genesis():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    with ThreadPoolExecutor(max_workers=8) as executor:
        installed = list(executor.map(
            lambda _: ensure_greenfield_activation(client, "org"), range(24),
        ))
    assert installed.count(True) == 1
    assert installed.count(False) == 23
    assert read_activation_baseline(client, "org")
    assert client.dbsize() == 1


def test_explicit_existing_estate_activation_remains_accepted(tmp_path):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    client.hset(RK.rbac_role_doc("org", "legacy", "reader"), "role", "reader")
    document = {
        "version": 1,
        "kind": "supertable_privileged_activation_baseline",
        "organization": "org",
        "activation_id": "explicit-existing-estate-cutover",
        "created_ms": 1_700_000_000_000,
        "state_sha256": compute_privileged_state_sha256(_outbox(client), "org"),
    }
    payload = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    path = tmp_path / "existing-estate.json"
    path.write_bytes(payload)
    report = verify_activation_baseline(
        str(path), expected_sha256=hashlib.sha256(payload).hexdigest(), organization="org",
    )
    assert attest_activation_baseline(_outbox(client), report)
    before = _snapshot(client)

    assert ensure_greenfield_activation(client, "org") is False
    assert read_activation_baseline(client, "org") == payload
    assert _snapshot(client) == before


def test_inert_failed_bootstrap_metadata_is_normalized_before_genesis():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    empty_state = compute_privileged_state_sha256(_outbox(client), "org")
    client.hset(RK.rbac_role_meta("org", "lake"), mapping={
        "version": "0", "last_updated_ms": "1234",
    })
    client.hset(RK.rbac_user_meta("org", "another-lake"), "version", "0")
    client.set(RK.meta_root("org", "lake"), "interrupted root")

    assert ensure_greenfield_activation(client, "org") is True
    assert not client.exists(RK.rbac_role_meta("org", "lake"))
    assert not client.exists(RK.rbac_user_meta("org", "another-lake"))
    assert client.get(RK.meta_root("org", "lake")) == "interrupted root"
    assert json.loads(read_activation_baseline(client, "org"))["state_sha256"] == empty_state
    assert compute_privileged_state_sha256(_outbox(client), "org") == empty_state


@pytest.mark.parametrize("state", [
    "role_document", "user_document", "role_index", "user_name_index",
    "role_revision", "malformed_meta", "extra_meta_field", "role_wrong_type",
    "token", "token_revision", "unknown_token_state", "outbox", "outbox_wrong_type",
    "audit_meta", "audit_delivery", "cascade",
])
def test_existing_unanchored_privileged_state_is_never_reclassified_as_empty(state):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    # Even normalization of these inert hashes must wait until all checks pass.
    client.hset(RK.rbac_role_meta("org", "retry"), "version", "0")
    if state == "role_document":
        client.hset(RK.rbac_role_doc("org", "other", "reader"), "role", "reader")
    elif state == "user_document":
        client.hset(RK.rbac_user_doc("org", "other", "user"), "username", "user")
    elif state == "role_index":
        client.sadd(RK.rbac_role_index("org", "other"), "reader")
    elif state == "user_name_index":
        client.hset(RK.rbac_username_to_id("org", "other"), "user", "user-id")
    elif state == "role_revision":
        client.hset(RK.rbac_role_meta("org", "other"), "version", "1")
    elif state == "malformed_meta":
        client.hset(RK.rbac_role_meta("org", "other"), mapping={
            "version": "0", "last_updated_ms": "invalid",
        })
    elif state == "extra_meta_field":
        client.hset(RK.rbac_role_meta("org", "other"), mapping={
            "version": "0", "extra": "state",
        })
    elif state == "role_wrong_type":
        client.set(RK.rbac_role_meta("org", "other"), "0")
    elif state == "token":
        client.hset(RK.auth_tokens("org"), "token", "secret")
    elif state == "token_revision":
        client.hset(RK.auth_tokens("org") + ":audit_meta", "version", "1")
    elif state == "unknown_token_state":
        client.set(RK.auth_tokens("org") + ":unexpected", "state")
    elif state == "outbox":
        client.xadd(RK.audit_privileged_outbox("org"), {"event": "existing"})
    elif state == "outbox_wrong_type":
        client.set(RK.audit_privileged_outbox("org"), "wrong type")
    elif state == "audit_meta":
        client.hset(RK.audit_privileged_meta("org"), "sequence", "0")
    elif state == "audit_delivery":
        client.hset(RK.audit_privileged_delivery("org"), "delivered", "record")
    elif state == "cascade":
        client.hset(RK.audit_privileged_cascade("org", "event"), "user", "record")
    before = _snapshot(client)

    with pytest.raises(ActivationBaselineError, match="existing-estate activation"):
        ensure_greenfield_activation(client, "org")
    assert _snapshot(client) == before


def test_atomic_scan_detects_state_created_immediately_before_installation():
    client = fakeredis.FakeStrictRedis(decode_responses=True)

    class InterleavingClient:
        def eval(self, *args):
            client.hset(RK.auth_tokens("org"), "concurrent-token", "secret")
            return client.eval(*args)

    with pytest.raises(ActivationBaselineError, match="existing-estate activation"):
        ensure_greenfield_activation(InterleavingClient(), "org")
    assert not client.exists(RK.audit_privileged_activation("org"))
    assert client.hget(RK.auth_tokens("org"), "concurrent-token") == "secret"


def test_other_organizations_do_not_block_greenfield_activation():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    client.hset(RK.rbac_role_doc("another-org", "lake", "reader"), "role", "reader")
    client.hset(RK.auth_tokens("another-org"), "token", "secret")
    client.xadd(RK.audit_privileged_outbox("another-org"), {"event": "existing"})
    before = _snapshot(client)
    assert ensure_greenfield_activation(client, "org") is True
    client.delete(RK.audit_privileged_activation("org"))
    assert _snapshot(client) == before


def test_precreated_empty_outbox_requires_explicit_activation():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    key = RK.audit_privileged_outbox("org")
    client.xgroup_create(key, "archive", mkstream=True)
    before = _snapshot(client)
    with pytest.raises(ActivationBaselineError, match="existing-estate activation"):
        ensure_greenfield_activation(client, "org")
    assert _snapshot(client) == before


@pytest.mark.parametrize("removal", ["delete", "trim", "pending_delete"])
def test_empty_outbox_with_past_events_cannot_restart_genesis(removal):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    key = RK.audit_privileged_outbox("org")
    stream_id = client.xadd(key, {"event": "past-audit-record"})
    if removal == "pending_delete":
        client.xgroup_create(key, "archive", id="0-0")
        client.xreadgroup("archive", "worker", {key: ">"})
    if removal == "trim":
        client.xtrim(key, maxlen=0)
    else:
        client.xdel(key, stream_id)
    assert client.xlen(key) == 0
    before = _snapshot(client)

    with pytest.raises(ActivationBaselineError, match="existing-estate activation"):
        ensure_greenfield_activation(client, "org")
    assert _snapshot(client) == before


@pytest.mark.parametrize("malformation", [
    "wrong_type", "invalid_json", "wrong_org", "invalid_hash", "inconsistent_hash",
    "invalid_time", "extra_field", "noncanonical",
])
def test_malformed_existing_anchor_is_rejected_without_replacement(malformation):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    assert ensure_greenfield_activation(client, "org")
    key = RK.audit_privileged_activation("org")
    document = json.loads(client.get(key))
    if malformation == "wrong_type":
        client.delete(key)
        client.hset(key, "invalid", "anchor")
    elif malformation == "invalid_json":
        client.set(key, "not-json")
    elif malformation == "noncanonical":
        client.set(key, json.dumps(document, indent=2))
    else:
        mutations = {
            "wrong_org": {"organization": "another-org"},
            "invalid_hash": {"artifact_sha256": "invalid"},
            "inconsistent_hash": {"artifact_sha256": "0" * 64},
            "invalid_time": {"created_ms": True},
            "extra_field": {"extra": "unexpected"},
        }
        document.update(mutations[malformation])
        client.set(key, json.dumps(document, sort_keys=True, separators=(",", ":")))
    before = _snapshot(client)
    with pytest.raises(ActivationBaselineError, match="invalid"):
        ensure_greenfield_activation(client, "org")
    assert _snapshot(client) == before


def test_lost_anchor_cannot_be_recreated_over_a_ledger():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    assert ensure_greenfield_activation(client, "org")
    client.xadd(RK.audit_privileged_outbox("org"), {"event": "committed"})
    client.delete(RK.audit_privileged_activation("org"))
    before = _snapshot(client)
    with pytest.raises(ActivationBaselineError, match="existing-estate activation"):
        ensure_greenfield_activation(client, "org")
    assert _snapshot(client) == before


def test_scan_bound_is_fail_closed(monkeypatch):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    monkeypatch.setattr(bootstrap, "MAX_GREENFIELD_SCAN_CALLS", 1)
    monkeypatch.setattr(bootstrap, "_SCAN_COUNT", 1)
    for index in range(20):
        client.set(f"unrelated:{index}", "value")
    before = _snapshot(client)
    with pytest.raises(ActivationBaselineError, match="exceeded its bound"):
        ensure_greenfield_activation(client, "org")
    assert _snapshot(client) == before


def test_backend_failure_does_not_leak_backend_error_text():
    class UnavailableRedis:
        def eval(self, *args):
            raise RuntimeError("secret backend connection detail")

    with pytest.raises(ActivationBaselineError, match="error_type=RuntimeError") as failure:
        ensure_greenfield_activation(UnavailableRedis(), "org")
    assert "secret" not in str(failure.value)
