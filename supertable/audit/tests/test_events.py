# route: supertable.audit.tests.test_events
"""Tests for ``supertable.audit.events``.

Covers:
  - Enum values and ``str`` semantics for all classification enums
  - ``_uuid7`` ordering, uniqueness, and format
  - ``INSTANCE_ID`` shape
  - ``AuditEvent`` defaults, immutability, ``to_dict`` / ``to_json`` /
    ``from_dict`` round-trips, and ``event_hash`` determinism
  - ``make_detail`` serialization rules (None drop, str fallback)
  - The ``Actions`` constants are unique and stable
"""
from __future__ import annotations

import dataclasses
import json
import re
import time
from typing import Set

import pytest

from supertable.audit import events as ev
from supertable.audit.events import (
    Actions,
    ActorType,
    AuditEvent,
    EventCategory,
    INSTANCE_ID,
    Outcome,
    Severity,
    _uuid7,
    make_detail,
)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class TestEnums:
    @pytest.mark.parametrize(
        "value",
        [
            EventCategory.AUTHENTICATION,
            EventCategory.AUTHORIZATION,
            EventCategory.DATA_ACCESS,
            EventCategory.DATA_MUTATION,
            EventCategory.RBAC_CHANGE,
            EventCategory.CONFIG_CHANGE,
            EventCategory.TOKEN_MGMT,
            EventCategory.SYSTEM,
            EventCategory.EXPORT,
            EventCategory.SECURITY_ALERT,
        ],
    )
    def test_event_category_is_str_enum(self, value: EventCategory) -> None:
        assert isinstance(value, str)
        assert value.value == value  # str enum: value equals self

    def test_severity_values(self) -> None:
        assert Severity.INFO.value == "info"
        assert Severity.WARNING.value == "warning"
        assert Severity.CRITICAL.value == "critical"

    def test_outcome_values(self) -> None:
        assert {o.value for o in Outcome} == {"success", "failure", "denied"}

    def test_actor_type_values(self) -> None:
        assert {a.value for a in ActorType} == {
            "user",
            "superuser",
            "api_token",
            "system",
            "mcp",
        }


# ---------------------------------------------------------------------------
# Instance identity
# ---------------------------------------------------------------------------


class TestInstanceId:
    def test_format_is_host_dash_pid(self) -> None:
        assert "-" in INSTANCE_ID
        host, pid = INSTANCE_ID.rsplit("-", 1)
        assert host
        assert pid.isdigit()

    def test_helper_returns_unknown_when_hostname_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def raising_hostname() -> str:
            raise OSError("boom")

        monkeypatch.setattr(ev.socket, "gethostname", raising_hostname)
        result = ev._build_instance_id()
        assert result.startswith("unknown-")


# ---------------------------------------------------------------------------
# _uuid7
# ---------------------------------------------------------------------------


_UUID7_RE = re.compile(r"^[0-9a-f]{12}-[0-9a-f]{4}-[0-9a-f]{8}$")


class TestUuid7:
    def test_format(self) -> None:
        assert _UUID7_RE.match(_uuid7())

    def test_unique_in_a_burst(self) -> None:
        seen: Set[str] = {_uuid7() for _ in range(1000)}
        assert len(seen) == 1000

    def test_lexicographic_ordering(self) -> None:
        first = _uuid7()
        # Tight loop produces many IDs; the ms field may collide but the counter
        # field always advances, so lexicographic ordering still holds.
        ids = [_uuid7() for _ in range(50)]
        assert all(first < x for x in ids)
        assert ids == sorted(ids)


# ---------------------------------------------------------------------------
# AuditEvent
# ---------------------------------------------------------------------------


class TestAuditEvent:
    def test_defaults(self) -> None:
        before_ms = int(time.time() * 1000)
        event = AuditEvent()
        after_ms = int(time.time() * 1000)

        assert _UUID7_RE.match(event.event_id)
        assert before_ms <= event.timestamp_ms <= after_ms + 5
        assert event.category == ""
        assert event.action == ""
        assert event.severity == Severity.INFO
        assert event.outcome == Outcome.SUCCESS
        assert event.actor_type == ActorType.SYSTEM
        assert event.chain_hash == ""
        assert event.instance_id == INSTANCE_ID

    def test_is_frozen(self) -> None:
        event = AuditEvent(action="x")
        with pytest.raises(dataclasses.FrozenInstanceError):
            event.action = "y"  # type: ignore[misc]

    def test_to_dict_round_trip(self) -> None:
        original = AuditEvent(
            category=EventCategory.RBAC_CHANGE,
            action=Actions.ROLE_CREATE,
            organization="acme",
            actor_username="alice",
            resource_type="role",
            resource_id="role_42",
            detail=make_detail(role_name="analyst"),
        )
        d = original.to_dict()
        assert d["organization"] == "acme"
        assert d["action"] == "role_create"
        rebuilt = AuditEvent.from_dict(d)
        assert rebuilt == original

    def test_from_dict_drops_unknown_keys(self) -> None:
        event = AuditEvent.from_dict(
            {
                "action": "x",
                "organization": "acme",
                "this_does_not_exist": True,
                "another_unknown": [1, 2, 3],
            }
        )
        assert event.action == "x"
        assert event.organization == "acme"

    def test_to_json_is_compact_and_parseable(self) -> None:
        event = AuditEvent(action="x", organization="acme", detail="hello")
        rendered = event.to_json()
        # No spaces between separators
        assert ", " not in rendered
        parsed = json.loads(rendered)
        assert parsed["organization"] == "acme"
        assert parsed["detail"] == "hello"

    def test_event_hash_is_deterministic(self) -> None:
        event = AuditEvent(action="x", organization="acme", timestamp_ms=1000)
        # Calling twice on the same event -> same hash
        assert event.event_hash() == event.event_hash()

    def test_event_hash_excludes_chain_hash_and_instance_id(self) -> None:
        # Two events identical except for chain_hash / instance_id should
        # hash to the same value, because event_hash is a content fingerprint
        # that is fed INTO the chain.
        common_kwargs = dict(
            event_id="same-id",
            timestamp_ms=1234,
            category="data_access",
            action="query_execute",
            organization="acme",
        )
        a = AuditEvent(**common_kwargs)
        # Build via from_dict so we can override the auto-generated instance_id.
        b_dict = a.to_dict()
        b_dict["chain_hash"] = "deadbeef" * 8
        b_dict["instance_id"] = "different-host-1"
        b = AuditEvent.from_dict(b_dict)

        assert a.event_hash() == b.event_hash()

    def test_event_hash_changes_with_content(self) -> None:
        a = AuditEvent(action="x", organization="acme", event_id="id-1", timestamp_ms=1)
        b = AuditEvent(action="y", organization="acme", event_id="id-1", timestamp_ms=1)
        assert a.event_hash() != b.event_hash()


# ---------------------------------------------------------------------------
# make_detail
# ---------------------------------------------------------------------------


class TestMakeDetail:
    def test_empty_returns_empty_string(self) -> None:
        assert make_detail() == ""

    def test_drops_none_values(self) -> None:
        rendered = make_detail(a=1, b=None, c="x")
        parsed = json.loads(rendered)
        assert parsed == {"a": 1, "c": "x"}

    def test_passes_through_jsonable_types(self) -> None:
        rendered = make_detail(
            i=1,
            f=1.5,
            b=True,
            s="hello",
            lst=[1, 2, 3],
            dct={"k": "v"},
        )
        parsed = json.loads(rendered)
        assert parsed == {
            "i": 1,
            "f": 1.5,
            "b": True,
            "s": "hello",
            "lst": [1, 2, 3],
            "dct": {"k": "v"},
        }

    def test_falls_back_to_str_for_unknown_types(self) -> None:
        class Marker:
            def __str__(self) -> str:
                return "marker-repr"

        rendered = make_detail(obj=Marker())
        parsed = json.loads(rendered)
        assert parsed == {"obj": "marker-repr"}

    def test_compact_output(self) -> None:
        # Same compactness contract as AuditEvent.to_json
        rendered = make_detail(a=1, b="two")
        assert ", " not in rendered


# ---------------------------------------------------------------------------
# Actions constants
# ---------------------------------------------------------------------------


class TestActionConstants:
    def test_constants_are_unique(self) -> None:
        values = [
            v
            for k, v in vars(Actions).items()
            if not k.startswith("_") and isinstance(v, str)
        ]
        assert len(values) == len(set(values)), "Duplicate Action values"

    def test_constants_are_lowercase_snake_case(self) -> None:
        bad = [
            v
            for k, v in vars(Actions).items()
            if not k.startswith("_") and isinstance(v, str)
            and not re.match(r"^[a-z][a-z0-9_]*$", v)
        ]
        assert bad == [], f"Non-snake_case action values: {bad}"

    def test_well_known_action_values(self) -> None:
        # These are referenced from many call sites; pin them.
        assert Actions.LOGIN_SUCCESS == "login_success"
        assert Actions.QUERY_EXECUTE == "query_execute"
        assert Actions.DATA_WRITE == "data_write"
        assert Actions.ROLE_CREATE == "role_create"
        assert Actions.RETENTION_EXECUTE == "retention_execute"
