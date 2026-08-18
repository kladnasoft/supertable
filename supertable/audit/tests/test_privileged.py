"""Security-boundary tests for privileged-action audit records."""
from __future__ import annotations

import dataclasses
import json
import math

import pytest

from supertable.audit.events import ActorType, EventCategory, Outcome, Severity
from supertable.audit.privileged import (
    MAX_PRIVILEGED_RECORD_BYTES,
    PrivilegedActionContext,
    PrivilegedAuditRecord,
    build_record,
    canonical_security_sha256,
    verify_records,
)


def _context(**overrides):
    values = {
        "actor_type": "user",
        "actor_id": "user-7",
        "username": "alice",
        "ip": "192.0.2.7",
        "user_agent": "pytest",
        "correlation_id": "corr-7",
        "session_id": "session-7",
        "server": "api-1",
        "reason": "approved access change",
        "ticket_id": "SEC-7",
        "mutation_id": "mutation-7",
        "cause": "administrator_request",
    }
    values.update(overrides)
    return PrivilegedActionContext(**values)


def _record(**overrides):
    values = {
        "context": _context(),
        "organization": "acme",
        "super_name": "sales",
        "action": "role_update",
        "resource_type": "role",
        "resource_id": "analyst",
        "before_document": {"name": "analyst", "permissions": ["read"]},
        "after_document": {"name": "analyst", "permissions": ["read", "write"]},
        "before_version": 10,
        "after_version": 11,
        "changed_fields": ("permissions",),
        "role_ids_added": ("role-b",),
        "role_ids_removed": ("role-a",),
        "namespace_version": 20,
        "affected_count": 1,
        "ledger_sequence": 1,
        "event_id": "event-7",
        "timestamp_ms": 1_700_000_000_007,
    }
    values.update(overrides)
    return build_record(**values)


class TestPrivilegedActionContext:
    def test_coerce_accepts_instance_and_current_audit_aliases(self) -> None:
        context = _context()
        assert PrivilegedActionContext.coerce(context) is context

        mapped = PrivilegedActionContext.coerce(
            {
                "actor_type": ActorType.USER,
                "actor_id": "user-1",
                "actor_username": "alice",
                "actor_ip": "192.0.2.1",
                "actor_user_agent": "client",
            }
        )
        assert mapped.actor_type == "user"
        assert mapped.username == "alice"
        assert mapped.ip == "192.0.2.1"
        assert mapped.user_agent == "client"

    def test_none_is_explicitly_marked_unattributed(self) -> None:
        context = PrivilegedActionContext.coerce(None)
        assert context.actor_type == "system"
        assert context.actor_id == "legacy-unattributed"
        assert context.context_missing is True
        assert context.cause == "missing_context"

    def test_system_constructor_requires_a_reason(self) -> None:
        with pytest.raises(ValueError, match="reason is required"):
            PrivilegedActionContext.system("")
        context = PrivilegedActionContext.system(
            "scheduled reconciliation", mutation_id="m-1"
        )
        assert context.actor_type == "system"
        assert context.context_missing is False
        assert context.reason == "scheduled reconciliation"

    @pytest.mark.parametrize(
        "value, error",
        [
            ({"actor_type": "user"}, ValueError),
            ({"actor_type": "robot", "actor_id": "x"}, ValueError),
            ({"actor_type": "user", "actor_id": "x", "extra": 1}, ValueError),
            ({"actor_type": "user", "actor_id": "x", "context_missing": 1}, TypeError),
            ({"actor_type": "user", "actor_id": "x\nforged"}, ValueError),
            ({"actor_type": "user", "actor_id": "x" * 257}, ValueError),
        ],
    )
    def test_coerce_rejects_ambiguous_or_unbounded_input(self, value, error) -> None:
        with pytest.raises(error):
            PrivilegedActionContext.coerce(value)

    def test_alias_collision_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate"):
            PrivilegedActionContext.coerce(
                {
                    "actor_type": "user",
                    "actor_id": "u-1",
                    "username": "one",
                    "actor_username": "two",
                }
            )

    def test_context_is_frozen(self) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            _context().actor_id = "changed"  # type: ignore[misc]


class TestCanonicalSecurityHash:
    def test_mapping_order_does_not_change_digest(self) -> None:
        assert canonical_security_sha256({"b": 2, "a": [1, True]}) == (
            canonical_security_sha256({"a": [1, True], "b": 2})
        )
        assert len(canonical_security_sha256({"a": 1})) == 64

    def test_content_change_changes_digest(self) -> None:
        assert canonical_security_sha256({"allowed": True}) != (
            canonical_security_sha256({"allowed": False})
        )

    def test_hashing_is_not_limited_by_the_smaller_record_envelope(self) -> None:
        # Role policy documents may legitimately be much larger than their
        # digest-only 64-KiB audit envelope.
        document = {"policy": "x" * (MAX_PRIVILEGED_RECORD_BYTES + 1)}
        assert len(canonical_security_sha256(document)) == 64

    @pytest.mark.parametrize(
        "document",
        [
            {1: "non-string key"},
            {"value": object()},
            {"value": math.nan},
            {"value": math.inf},
            {"value": {"set"}},
        ],
    )
    def test_non_json_or_non_finite_documents_are_rejected(self, document) -> None:
        with pytest.raises((TypeError, ValueError)):
            canonical_security_sha256(document)


class TestBuildAndSerialization:
    def test_build_copies_context_and_keeps_only_document_digests(self) -> None:
        secret = "literal-secret-that-must-not-be-audited"
        before = {
            "tables": {
                "cards": {
                    "filter": {"column": "tenant", "literal": secret}
                }
            }
        }
        after = {"tables": {"cards": {"columns": ["id"]}}}
        record = _record(
            before_document=before,
            after_document=after,
            changed_fields=("tables.cards", "tables.cards"),
            role_ids_added=("role-z", "role-a", "role-a"),
        )

        assert record.actor_username == "alice"
        assert record.reason == "approved access change"
        assert record.before_sha256 == canonical_security_sha256(before)
        assert record.after_sha256 == canonical_security_sha256(after)
        assert record.changed_fields == ("tables.cards",)
        assert record.role_ids_added == ("role-a", "role-z")
        assert secret not in record.to_json()
        assert '"filter"' not in record.to_json()

    def test_large_role_delta_is_counted_and_hashed_without_growing_record(self) -> None:
        role_ids = tuple(f"role-{index:04d}" for index in range(300))
        record = _record(role_ids_added=role_ids)

        assert len(record.role_ids_added) == 16
        assert record.role_ids_added_count == 300
        assert len(record.role_ids_added_sha256) == 64
        assert len(record.to_json().encode("utf-8")) < MAX_PRIVILEGED_RECORD_BYTES

        tampered = record.to_dict()
        tampered["role_ids_added_sha256"] = "0" * 64
        with pytest.raises(ValueError, match="payload_hash"):
            PrivilegedAuditRecord.from_dict(tampered)

    def test_missing_context_is_visible_in_record(self) -> None:
        record = _record(context=None)
        assert record.context_missing is True
        assert record.actor_id == "legacy-unattributed"
        assert record.cause == "missing_context"

    def test_absent_documents_have_empty_digests(self) -> None:
        record = _record(before_document=None, after_document=None)
        assert record.before_sha256 == ""
        assert record.after_sha256 == ""

    def test_existing_enums_are_accepted_and_normalized(self) -> None:
        record = _record(outcome=Outcome.DENIED, severity=Severity.CRITICAL)
        assert record.outcome == "denied"
        assert record.severity == "critical"

        unchanged = _record(outcome=Outcome.NO_CHANGE)
        assert unchanged.outcome == "no_change"

    def test_json_round_trip_is_canonical_and_exact(self) -> None:
        record = _record()
        rendered = record.to_json()
        assert ", " not in rendered
        assert rendered == json.dumps(
            json.loads(rendered), sort_keys=True, separators=(",", ":")
        )
        assert PrivilegedAuditRecord.from_json(rendered) == record
        assert PrivilegedAuditRecord.from_json(rendered.encode("utf-8")) == record
        assert PrivilegedAuditRecord.from_dict(record.to_dict()) == record

    def test_commit_assigned_fields_do_not_invalidate_template_hash(self) -> None:
        template = _record(
            namespace_version=0,
            affected_count=0,
            ledger_sequence=0,
        )
        committed = template.to_dict()
        # Redis Lua encodes these as decimal strings, preserving exact values
        # above cjson/IEEE-754's safe integer range.
        committed["namespace_version"] = "45"
        committed["affected_count"] = "3"
        committed["ledger_sequence"] = "901"

        rebuilt = PrivilegedAuditRecord.from_dict(committed)
        assert rebuilt.payload_hash == template.payload_hash
        assert rebuilt.namespace_version == 45
        assert rebuilt.affected_count == 3
        assert rebuilt.ledger_sequence == 901
        assert PrivilegedAuditRecord.from_json(rebuilt.to_json()) == rebuilt

    @pytest.mark.parametrize(
        "field",
        ["namespace_version", "affected_count", "ledger_sequence"],
    )
    @pytest.mark.parametrize("bad_value", ["", "01", "+1", "-1", "1.0", " 1"])
    def test_lua_counter_strings_must_be_canonical_decimal(
        self, field, bad_value
    ) -> None:
        value = _record().to_dict()
        value[field] = bad_value
        with pytest.raises(ValueError, match="canonical unsigned decimal"):
            PrivilegedAuditRecord.from_dict(value)

    def test_direct_constructor_does_not_coerce_string_counters(self) -> None:
        value = _record().to_dict()
        value["ledger_sequence"] = "2"
        value["payload_hash"] = ""
        with pytest.raises(TypeError, match="ledger_sequence must be an integer"):
            PrivilegedAuditRecord(**value)

    def test_deserialization_rejects_non_array_collection_fields(self) -> None:
        value = _record().to_dict()
        value["role_ids_added"] = {"role-a": True}
        value["payload_hash"] = ""
        with pytest.raises(TypeError, match="JSON array"):
            PrivilegedAuditRecord.from_dict(value)

    def test_build_rejects_a_string_instead_of_a_field_collection(self) -> None:
        with pytest.raises(TypeError, match="iterable of strings"):
            _record(changed_fields="permissions")

    @pytest.mark.parametrize(
        "field, value, error",
        [
            ("event_id", "", ValueError),
            ("mutation_id", "", ValueError),
            ("timestamp_ms", 0, ValueError),
        ],
    )
    def test_deserialization_never_repairs_missing_identity_or_time(
        self, field, value, error
    ) -> None:
        raw = _record().to_dict()
        raw[field] = value
        raw["payload_hash"] = ""
        with pytest.raises(error):
            PrivilegedAuditRecord.from_dict(raw)

    def test_sequence_copy_preserves_payload_hash(self) -> None:
        template = _record(ledger_sequence=0)
        committed = template.with_ledger_sequence(99)
        assert committed.ledger_sequence == 99
        assert committed.payload_hash == template.payload_hash

    @pytest.mark.parametrize(
        "mutation",
        [
            lambda value: value.update({"action": "role_delete"}),
            lambda value: value.update({"actor_id": "attacker"}),
            lambda value: value.update({"after_sha256": "0" * 64}),
        ],
    )
    def test_payload_tampering_is_rejected(self, mutation) -> None:
        value = _record().to_dict()
        mutation(value)
        with pytest.raises(ValueError, match="payload_hash"):
            PrivilegedAuditRecord.from_dict(value)

    def test_unknown_missing_duplicate_and_oversize_json_are_rejected(self) -> None:
        value = _record().to_dict()
        value["unexpected"] = True
        with pytest.raises(ValueError, match="unknown"):
            PrivilegedAuditRecord.from_dict(value)

        value = _record().to_dict()
        value.pop("event_id")
        with pytest.raises(ValueError, match="missing"):
            PrivilegedAuditRecord.from_dict(value)

        rendered = _record().to_json()
        duplicate = rendered[:-1] + ',"event_id":"duplicate"}'
        with pytest.raises(ValueError, match="duplicate"):
            PrivilegedAuditRecord.from_json(duplicate)

        with pytest.raises(ValueError, match="64-KiB"):
            PrivilegedAuditRecord.from_json(" " * (MAX_PRIVILEGED_RECORD_BYTES + 1))

    @pytest.mark.parametrize("value", ["[]", "null", b"\xff", 1])
    def test_invalid_json_envelopes_are_rejected(self, value) -> None:
        with pytest.raises((TypeError, ValueError)):
            PrivilegedAuditRecord.from_json(value)

    def test_record_is_frozen(self) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            _record().action = "changed"  # type: ignore[misc]


class TestLedgerVerificationAndArchival:
    def test_contiguous_positive_records_verify(self) -> None:
        first = _record(ledger_sequence=40, event_id="event-40")
        second = _record(ledger_sequence=41, event_id="event-41")
        assert verify_records([first, second]) is True
        assert verify_records([first.to_json(), second.to_dict()], expected_start=40)
        assert verify_records([], expected_start=1) is True

    def test_gaps_zero_reordering_and_duplicates_fail(self) -> None:
        first = _record(ledger_sequence=40, event_id="event-40")
        second = _record(ledger_sequence=42, event_id="event-42")
        assert verify_records([first, second]) is False
        assert verify_records([_record(ledger_sequence=0)]) is False
        assert verify_records([second, first]) is False
        assert verify_records([first, first]) is False
        assert verify_records([first], expected_start=39) is False

    def test_conversion_to_existing_audit_event_is_digest_only(self) -> None:
        secret = "do-not-archive-this-filter-literal"
        record = _record(
            before_document={"filter": {"literal": secret}},
            after_document={"filter": None},
        )
        event = record.to_audit_event()
        detail = json.loads(event.detail)

        assert event.category == EventCategory.RBAC_CHANGE.value
        assert event.event_id == record.event_id
        assert event.actor_id == record.actor_id
        assert event.reason == record.reason
        assert detail["payload_hash"] == record.payload_hash
        assert detail["before_sha256"] == record.before_sha256
        assert secret not in event.detail
