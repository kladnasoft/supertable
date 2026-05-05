# route: supertable.audit.tests.test_retention
"""Tests for ``supertable.audit.retention``.

Focused on the pure logic that does not touch Redis or storage:
  - ``_legal_hold_key`` template substitution
  - ``_parse_partition_date`` accepts every valid Hive partition layout and
    rejects malformed paths
  - ``is_legal_hold_active`` resolution order (Redis → Settings → fail-safe)
  - ``set_legal_hold`` argument validation and Redis persistence
  - ``enforce_retention`` early-exit branches (legal hold, missing org,
    retention disabled, no partitions, partition enumeration errors)
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from supertable.audit import retention


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestLegalHoldKey:
    def test_template_substitution(self) -> None:
        assert retention._legal_hold_key("acme") == "supertable:acme:audit:legal_hold"

    def test_handles_special_characters(self) -> None:
        assert retention._legal_hold_key("acme-EU") == "supertable:acme-EU:audit:legal_hold"


class TestParsePartitionDate:
    @pytest.mark.parametrize(
        "path,expected",
        [
            ("audit/year=2024/month=03/day=15", datetime(2024, 3, 15, tzinfo=timezone.utc)),
            ("audit/year=2024/month=03/day=15/", datetime(2024, 3, 15, tzinfo=timezone.utc)),
            (r"audit\year=2024\month=03\day=15", datetime(2024, 3, 15, tzinfo=timezone.utc)),
            ("a/b/c/year=1999/month=12/day=31", datetime(1999, 12, 31, tzinfo=timezone.utc)),
        ],
    )
    def test_valid_partitions(self, path: str, expected: datetime) -> None:
        assert retention._parse_partition_date(path) == expected

    @pytest.mark.parametrize(
        "path",
        [
            "",
            "no-partition-here",
            "year=2024/month=03",                  # missing day
            "year=2024/month=03/day=",             # incomplete day
            "year=20XX/month=03/day=15",           # non-numeric year
            "year=2024/month=13/day=15",           # invalid month
            "year=2024/month=02/day=30",           # invalid day for month
        ],
    )
    def test_rejects_malformed(self, path: str) -> None:
        assert retention._parse_partition_date(path) is None


# ---------------------------------------------------------------------------
# Legal hold — query / set
# ---------------------------------------------------------------------------


def _make_redis_client(get_value: object | None = None) -> MagicMock:
    """Build a fake Redis client with deterministic get/set behavior."""
    client = MagicMock()
    client.get.return_value = get_value
    client.set.return_value = True
    return client


class TestIsLegalHoldActive:
    @pytest.mark.parametrize("redis_value", [b"1", "1", b"true", "yes", b"YES"])
    def test_redis_truthy(
        self, monkeypatch: pytest.MonkeyPatch, redis_value: object
    ) -> None:
        fake_client = _make_redis_client(get_value=redis_value)
        fake_module = SimpleNamespace(redis_client=fake_client)
        monkeypatch.setitem(__import__("sys").modules, "supertable.server_common", fake_module)
        assert retention.is_legal_hold_active("acme") is True

    @pytest.mark.parametrize("redis_value", [b"0", "false", b"no"])
    def test_redis_falsy(
        self, monkeypatch: pytest.MonkeyPatch, redis_value: object
    ) -> None:
        fake_client = _make_redis_client(get_value=redis_value)
        fake_module = SimpleNamespace(redis_client=fake_client)
        monkeypatch.setitem(__import__("sys").modules, "supertable.server_common", fake_module)
        assert retention.is_legal_hold_active("acme") is False

    def test_falls_back_to_settings_when_redis_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_client = _make_redis_client(get_value=None)
        fake_server_common = SimpleNamespace(redis_client=fake_client)
        monkeypatch.setitem(
            __import__("sys").modules,
            "supertable.server_common",
            fake_server_common,
        )

        import supertable.config.settings as settings_module
        monkeypatch.setattr(
            settings_module,
            "settings",
            SimpleNamespace(SUPERTABLE_AUDIT_LEGAL_HOLD=True),
            raising=True,
        )
        assert retention.is_legal_hold_active("acme") is True

    def test_fail_safe_when_everything_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Build a fake server_common module whose .redis_client raises on access.
        class BoomModule:
            @property
            def redis_client(self):
                raise RuntimeError("redis unreachable")

        monkeypatch.setitem(
            __import__("sys").modules, "supertable.server_common", BoomModule()
        )

        # Make settings import fail by removing the attribute
        import supertable.config.settings as settings_module
        monkeypatch.delattr(settings_module, "settings", raising=False)

        assert retention.is_legal_hold_active("acme") is True


class TestSetLegalHold:
    def test_requires_organization(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Clear the settings default org so resolution returns ""
        import supertable.config.settings as settings_module
        monkeypatch.setattr(
            settings_module,
            "settings",
            SimpleNamespace(SUPERTABLE_ORGANIZATION=""),
            raising=True,
        )
        result = retention.set_legal_hold(True, organization="")
        assert result == {"ok": False, "error": "organization required"}

    def test_persists_to_redis_and_returns_ok(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_client = _make_redis_client()
        fake_module = SimpleNamespace(redis_client=fake_client)
        monkeypatch.setitem(__import__("sys").modules, "supertable.server_common", fake_module)

        # Stub out the audit emit chain so the test never touches the real logger.
        import supertable.audit as audit_pkg

        audit_calls = []

        def fake_emit(**kwargs):
            audit_calls.append(kwargs)

        monkeypatch.setattr(audit_pkg, "emit", fake_emit, raising=True)

        result = retention.set_legal_hold(True, organization="acme")
        assert result == {"ok": True, "legal_hold": True, "organization": "acme"}

        fake_client.set.assert_called_once_with(
            "supertable:acme:audit:legal_hold", "1"
        )
        assert audit_calls and audit_calls[0]["organization"] == "acme"

    def test_redis_failure_returns_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_client = MagicMock()
        fake_client.set.side_effect = RuntimeError("redis down")
        fake_module = SimpleNamespace(redis_client=fake_client)
        monkeypatch.setitem(__import__("sys").modules, "supertable.server_common", fake_module)

        result = retention.set_legal_hold(False, organization="acme")
        assert result["ok"] is False
        assert "redis down" in result["error"]


# ---------------------------------------------------------------------------
# enforce_retention — early-exit branches
# ---------------------------------------------------------------------------


class TestEnforceRetention:
    def test_empty_organization_short_circuits(self) -> None:
        result = retention.enforce_retention("")
        assert result["deleted_partitions"] == 0
        assert "organization is empty" in result["errors"]

    def test_legal_hold_skips_deletion(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(retention, "is_legal_hold_active", lambda org: True)

        result = retention.enforce_retention("acme")
        assert result["skipped_legal_hold"] is True
        assert result["deleted_partitions"] == 0

    def test_zero_retention_days_means_disabled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(retention, "is_legal_hold_active", lambda org: False)

        import supertable.config.settings as settings_module
        monkeypatch.setattr(
            settings_module,
            "settings",
            SimpleNamespace(SUPERTABLE_AUDIT_RETENTION_DAYS=0),
            raising=True,
        )

        result = retention.enforce_retention("acme")
        assert result["retention_days"] == 0
        assert result["deleted_partitions"] == 0

    def test_partition_enumeration_failure_recorded(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(retention, "is_legal_hold_active", lambda org: False)

        import supertable.config.settings as settings_module
        monkeypatch.setattr(
            settings_module,
            "settings",
            SimpleNamespace(SUPERTABLE_AUDIT_RETENTION_DAYS=30),
            raising=True,
        )

        # Replace ParquetAuditWriter to raise on list_partitions
        import supertable.audit.writer_parquet as wp

        class BoomWriter:
            def list_partitions(self, organization: str) -> list[str]:
                raise RuntimeError("partition listing exploded")

        monkeypatch.setattr(wp, "ParquetAuditWriter", BoomWriter, raising=True)

        result = retention.enforce_retention("acme")
        assert result["deleted_partitions"] == 0
        assert any("partition enumeration failed" in e for e in result["errors"])

    def test_deletes_only_partitions_older_than_cutoff(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(retention, "is_legal_hold_active", lambda org: False)

        import supertable.config.settings as settings_module
        monkeypatch.setattr(
            settings_module,
            "settings",
            SimpleNamespace(SUPERTABLE_AUDIT_RETENTION_DAYS=10),
            raising=True,
        )

        old_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime(
            "audit/year=%Y/month=%m/day=%d"
        )
        recent_date = (datetime.now(timezone.utc) - timedelta(days=5)).strftime(
            "audit/year=%Y/month=%m/day=%d"
        )
        unparseable = "audit/random/path"

        # Order matters: the implementation breaks out on the first
        # not-yet-old partition, so we hand it sorted oldest→newest.
        partitions = [old_date, unparseable, recent_date]

        import supertable.audit.writer_parquet as wp

        class FakeWriter:
            def list_partitions(self, organization: str) -> list[str]:
                return partitions

        monkeypatch.setattr(wp, "ParquetAuditWriter", FakeWriter, raising=True)

        deleted_paths: list[str] = []

        class FakeStorage:
            def delete(self, path: str) -> None:
                deleted_paths.append(path)

        import supertable.storage.storage_factory as sf

        monkeypatch.setattr(sf, "get_storage", lambda: FakeStorage(), raising=True)

        # Stub audit emit to avoid touching the real logger
        import supertable.audit as audit_pkg
        monkeypatch.setattr(audit_pkg, "emit", lambda **kwargs: None, raising=True)

        result = retention.enforce_retention("acme")

        assert result["deleted_partitions"] == 1
        assert deleted_paths == [old_date]
        assert result["deleted_paths"] == [old_date]
