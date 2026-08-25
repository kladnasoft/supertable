# route: supertable.audit.tests.test_reader_integrity
"""Fail-closed, content-bound audit-chain verification regressions."""
from __future__ import annotations

import hashlib
import json
import sys
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from supertable.audit import reader
from supertable.audit.chain import (
    GENESIS_HASH,
    InstanceChain,
    MerkleProof,
    compute_chain_hash,
    compute_event_batch_hash,
)


def _event(
    event_id: str,
    *,
    timestamp_ms: int,
    instance_id: str = "audit-instance-1",
    detail: str = "redacted",
) -> dict:
    return {
        "event_id": event_id,
        "timestamp_ms": timestamp_ms,
        "category": "data_access",
        "action": "query.execute",
        "severity": "info",
        "actor_type": "user",
        "actor_id": "actor-1",
        "actor_username": "user",
        "actor_ip": "",
        "actor_user_agent": "",
        "organization": "acme",
        "super_name": "lake",
        "correlation_id": "correlation-1",
        "session_id": "",
        "server": "",
        "resource_type": "query",
        "resource_id": "query-1",
        "detail": detail,
        "outcome": "success",
        "reason": "",
        "chain_hash": "",
        "instance_id": instance_id,
    }


def _batch(previous_head: str, events: list[dict], *, file_name: str) -> dict:
    head = compute_chain_hash(previous_head, compute_event_batch_hash(events))
    stored = deepcopy(events)
    for event in stored:
        event["chain_hash"] = head
    return {
        "file_path": f"acme/__audit__/year=2026/month=08/day=24/{file_name}",
        "file_hash": "f" * 64,
        "events": stored,
        "chain_hash": head,
        "instance_id": stored[0]["instance_id"],
        "event_ids": sorted(event["event_id"] for event in stored),
        "event_count": len(stored),
        "min_timestamp_ms": min(event["timestamp_ms"] for event in stored),
    }


def _proof(
    *,
    date: str,
    instance_id: str,
    head: str,
    batches: int,
    events: int,
) -> MerkleProof:
    proof = MerkleProof(date=date)
    proof.add_instance(
        InstanceChain(instance_id, head=head, batch_count=batches),
        event_count=events,
    )
    proof.compute_root()
    return proof


def _install_writer(
    monkeypatch: pytest.MonkeyPatch,
    *,
    batches: list[dict],
    current: MerkleProof | None,
    previous: MerkleProof | None = None,
    read_error: Exception | None = None,
    previous_hash_override: str | None = None,
) -> None:
    import supertable.audit.writer_parquet as writer_parquet

    class FakeWriter:
        def read_batch_events(self, *_args, **_kwargs):
            if read_error is not None:
                raise read_error
            return deepcopy(batches)

        def load_chain_proof(self, _org, date, **_kwargs):
            clean = date.replace("-", "")
            if clean == "20260824":
                return deepcopy(current)
            if clean == "20260823":
                return deepcopy(previous)
            return None

        def load_day_close_manifest(self, _org, date, **_kwargs):
            clean = date.replace("-", "")
            selected = current if clean == "20260824" else previous
            selected_batches = batches if clean == "20260824" else []
            if clean not in {"20260824", "20260823"}:
                return None
            proof_hash = "0" * 64
            if selected is not None:
                proof_hash = hashlib.sha256(json.dumps(
                    selected.to_dict(),
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8")).hexdigest()
            if clean == "20260823" and previous_hash_override is not None:
                proof_hash = previous_hash_override
            return {
                "admitted": sum(
                    batch.get("event_count", 0)
                    for batch in selected_batches
                ),
                "batch_ids": [
                    batch["file_path"].rsplit("/", 1)[-1]
                    .removesuffix(".parquet").rsplit("_", 1)[-1]
                    for batch in selected_batches
                ],
                "cutover_day": 20_323 if previous is not None else 20_324,
                "day": 20_324 if clean == "20260824" else 20_323,
                "receipt_count": len(selected_batches),
                "proof_hash": proof_hash,
            }

    monkeypatch.setattr(
        writer_parquet, "ParquetAuditWriter", FakeWriter, raising=True,
    )


def _one_batch_fixture() -> tuple[list[dict], MerkleProof]:
    batch = _batch(
        GENESIS_HASH,
        [_event("event-1", timestamp_ms=1)],
        file_name="audit_valid.parquet",
    )
    proof = _proof(
        date="2026-08-24",
        instance_id="audit-instance-1",
        head=batch["chain_hash"],
        batches=1,
        events=1,
    )
    return [batch], proof


def test_verifies_complete_content_bound_genesis_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batches, proof = _one_batch_fixture()
    _install_writer(monkeypatch, batches=batches, current=proof)

    result = reader.verify_chain_integrity("acme", "2026-08-24")

    assert result["valid"] is True
    assert result["status"] == "verified"
    assert result["total_batches"] == 1
    assert result["total_events"] == 1
    assert result["instances"]["audit-instance-1"]["chain_valid"] is True


def test_missing_current_proof_invalidates_closed_empty_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_writer(monkeypatch, batches=[], current=None)

    result = reader.verify_chain_integrity("acme", "2026-08-24")

    assert result["valid"] is False
    assert result["status"] == "invalid"
    assert result["error"] == "Closed audit day proof is unavailable"


def test_tampered_event_content_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batches, proof = _one_batch_fixture()
    batches[0]["events"][0]["detail"] = "tampered-sensitive-content"
    _install_writer(monkeypatch, batches=batches, current=proof)

    result = reader.verify_chain_integrity("acme", "2026-08-24")

    assert result["valid"] is False
    assert result["status"] == "invalid"


@pytest.mark.parametrize(
    "mutation",
    [
        "event_instance",
        "event_chain",
        "proof_head",
        "proof_batch_count",
        "proof_event_count",
    ],
)
def test_batch_identity_and_proof_mismatches_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    batches, proof = _one_batch_fixture()
    if mutation == "event_instance":
        batches[0]["events"][0]["instance_id"] = "audit-instance-2"
    elif mutation == "event_chain":
        batches[0]["events"][0]["chain_hash"] = "a" * 64
    elif mutation == "proof_head":
        proof.instances["audit-instance-1"]["head"] = "a" * 64
        proof.compute_root()
    elif mutation == "proof_batch_count":
        proof.instances["audit-instance-1"]["batches"] = 2
    else:
        proof.instances["audit-instance-1"]["events"] = 2
        proof.total_events = 2
    _install_writer(monkeypatch, batches=batches, current=proof)

    result = reader.verify_chain_integrity("acme", "2026-08-24")

    assert result["valid"] is False
    assert result["status"] == "invalid"


def test_duplicate_event_ids_across_batches_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _batch(
        GENESIS_HASH,
        [_event("duplicate", timestamp_ms=1)],
        file_name="audit_first.parquet",
    )
    second = _batch(
        first["chain_hash"],
        [_event("duplicate", timestamp_ms=2)],
        file_name="audit_second.parquet",
    )
    proof = _proof(
        date="2026-08-24",
        instance_id="audit-instance-1",
        head=second["chain_hash"],
        batches=2,
        events=2,
    )
    _install_writer(monkeypatch, batches=[first, second], current=proof)

    result = reader.verify_chain_integrity("acme", "2026-08-24")

    assert result["valid"] is False
    assert result["status"] == "invalid"


def test_previous_proof_anchors_cross_day_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous_head = "b" * 64
    batch = _batch(
        previous_head,
        [_event("event-2", timestamp_ms=2)],
        file_name="audit_cross_day.parquet",
    )
    previous = _proof(
        date="2026-08-23",
        instance_id="audit-instance-1",
        head=previous_head,
        batches=4,
        events=3,
    )
    current = _proof(
        date="2026-08-24",
        instance_id="audit-instance-1",
        head=batch["chain_hash"],
        batches=5,
        events=1,
    )
    _install_writer(
        monkeypatch, batches=[batch], current=current, previous=previous,
    )

    result = reader.verify_chain_integrity("acme", "2026-08-24")

    assert result["valid"] is True
    assert result["instances"]["audit-instance-1"]["starting_head"] == previous_head


def test_previous_proof_must_match_its_immutable_close_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous_head = "b" * 64
    batch = _batch(
        previous_head,
        [_event("event-2", timestamp_ms=2)],
        file_name="audit_cross_day.parquet",
    )
    previous = _proof(
        date="2026-08-23",
        instance_id="audit-instance-1",
        head=previous_head,
        batches=4,
        events=3,
    )
    current = _proof(
        date="2026-08-24",
        instance_id="audit-instance-1",
        head=batch["chain_hash"],
        batches=5,
        events=1,
    )
    _install_writer(
        monkeypatch,
        batches=[batch],
        current=current,
        previous=previous,
        previous_hash_override="0" * 64,
    )

    result = reader.verify_chain_integrity("acme", "2026-08-24")

    assert result["valid"] is False
    assert result["status"] == "invalid"
    assert "Previous audit proof differs" in result["error"]


@pytest.mark.parametrize(
    "journal_state,expected_status",
    [
        ({"status": "closing"}, "closing"),
        ({}, "unverifiable_unclosed"),
    ],
)
def test_unclosed_historical_day_reports_exact_closure_state_without_activation(
    monkeypatch: pytest.MonkeyPatch,
    journal_state: dict[str, str],
    expected_status: str,
) -> None:
    import supertable.audit.writer_parquet as writer_parquet

    class FakeWriter:
        def load_day_close_manifest(self, *_args, **_kwargs):
            return None

    class ReadOnlyRedis:
        def __init__(self):
            self.eval_calls = 0

        def eval(self, *_args, **_kwargs):
            self.eval_calls += 1
            raise AssertionError("verification must not activate a journal")

        def hgetall(self, _key):
            return journal_state

        def hmget(self, _key, _fields):
            return [None, None, None]

    backend = ReadOnlyRedis()
    monkeypatch.setattr(writer_parquet, "ParquetAuditWriter", FakeWriter)
    monkeypatch.setitem(
        sys.modules,
        "supertable.redis_infra",
        SimpleNamespace(redis_client=backend),
    )

    result = reader.verify_chain_integrity("acme", "2020-01-01")

    assert result["status"] == expected_status
    assert backend.eval_calls == 0


def test_current_open_day_is_not_reported_as_missing_closed_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import supertable.audit.writer_parquet as writer_parquet

    class FakeWriter:
        def load_day_close_manifest(self, *_args, **_kwargs):
            return None

    class ReadOnlyRedis:
        def hgetall(self, _key):
            return {}

        def hmget(self, _key, _fields):
            return [None, None, None]

    monkeypatch.setattr(writer_parquet, "ParquetAuditWriter", FakeWriter)
    monkeypatch.setitem(
        sys.modules,
        "supertable.redis_infra",
        SimpleNamespace(redis_client=ReadOnlyRedis()),
    )
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    result = reader.verify_chain_integrity("acme", today)

    assert result["status"] == "open"


def test_closed_journal_day_missing_manifest_is_invalid_not_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import supertable.audit.writer_parquet as writer_parquet

    class FakeWriter:
        def load_day_close_manifest(self, *_args, **_kwargs):
            return None

    target = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp()) // 86_400

    class ReadOnlyRedis:
        def hgetall(self, _key):
            return {}

        def hmget(self, _key, _fields):
            return ["1", str(target), str(target)]

    monkeypatch.setattr(writer_parquet, "ParquetAuditWriter", FakeWriter)
    monkeypatch.setitem(
        sys.modules,
        "supertable.redis_infra",
        SimpleNamespace(redis_client=ReadOnlyRedis()),
    )

    result = reader.verify_chain_integrity("acme", "2020-01-01")

    assert result["status"] == "invalid"
    assert "missing its immutable manifest" in result["error"]


def test_missing_predecessor_rejects_non_genesis_cumulative_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batches, proof = _one_batch_fixture()
    proof.instances["audit-instance-1"]["batches"] = 7
    _install_writer(monkeypatch, batches=batches, current=proof)

    result = reader.verify_chain_integrity("acme", "2026-08-24")

    assert result["valid"] is False
    assert result["status"] == "invalid"


def test_strict_read_failure_never_becomes_an_empty_valid_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_writer(
        monkeypatch,
        batches=[],
        current=None,
        read_error=RuntimeError("s3://token@example.invalid/private"),
    )

    result = reader.verify_chain_integrity("acme", "2026-08-24")

    assert result["valid"] is False
    assert result["error"] == "Audit integrity inputs could not be read"
    assert "token" not in str(result)


@pytest.mark.parametrize(
    "organization,date",
    [
        ("../../escape", "2026-08-24"),
        ("acme", "2026-8-24"),
        ("acme", " 2026-08-24"),
        ("acme\\escape", "20260824"),
    ],
)
def test_invalid_scope_or_date_is_rejected_before_storage(
    monkeypatch: pytest.MonkeyPatch,
    organization: str,
    date: str,
) -> None:
    import supertable.audit.writer_parquet as writer_parquet

    class MustNotConstruct:
        def __init__(self):
            raise AssertionError("storage must not be constructed")

    monkeypatch.setattr(
        writer_parquet, "ParquetAuditWriter", MustNotConstruct, raising=True,
    )

    result = reader.verify_chain_integrity(organization, date)

    assert result["valid"] is False
    assert result["error"] == "Invalid audit verification request"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"limit": 0},
        {"limit": 10_001},
        {"limit": True},
        {"source": "unknown"},
        {"start_ms": -1},
        {"end_ms": 253_402_300_800_000},
        {"start_ms": 2, "end_ms": 1},
        {"actor_id": "x" * 1_025},
    ],
)
def test_query_rejects_invalid_bounds_before_backend_access(
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict,
) -> None:
    monkeypatch.setattr(
        reader, "_query_redis",
        lambda *_args, **_kwargs: pytest.fail("backend must not be called"),
    )
    monkeypatch.setattr(
        reader, "_query_parquet",
        lambda *_args, **_kwargs: pytest.fail("backend must not be called"),
    )

    with pytest.raises(ValueError):
        reader.query_audit_log("acme", **kwargs)


def test_archive_query_rejects_more_than_31_partitions_before_writer_init(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import supertable.audit.writer_parquet as writer_parquet

    class MustNotConstruct:
        def __init__(self):
            raise AssertionError("writer must not be constructed")

    monkeypatch.setattr(
        writer_parquet, "ParquetAuditWriter", MustNotConstruct, raising=True,
    )

    with pytest.raises(ValueError, match="day-partition limit"):
        reader.query_audit_log(
            "acme",
            source="parquet",
            start_ms=0,
            end_ms=32 * 24 * 3600 * 1000,
        )


def test_archive_query_failure_is_sanitized_and_never_partial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import supertable.audit.writer_parquet as writer_parquet

    secret = "s3://access:secret@example.invalid/private"

    class FailingWriter:
        def read_batch_events(self, *_args, **_kwargs):
            raise RuntimeError(secret)

    monkeypatch.setattr(
        writer_parquet, "ParquetAuditWriter", FailingWriter, raising=True,
    )

    with pytest.raises(reader.AuditQueryError) as caught:
        reader.query_audit_log(
            "acme", source="parquet", start_ms=0, end_ms=1,
        )

    assert secret not in str(caught.value)


def test_auto_query_with_historical_end_routes_exact_range_to_archive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = 2_000_000_000_000
    historical_end = now_ms - (48 * 3600 * 1000)
    calls: list[tuple] = []
    monkeypatch.setattr(reader.time, "time", lambda: now_ms / 1000)
    monkeypatch.setattr(
        reader,
        "_query_parquet",
        lambda org, start, end, limit: calls.append(
            ("parquet", org, start, end, limit)
        ) or [],
    )
    monkeypatch.setattr(
        reader,
        "_query_redis",
        lambda *_args: pytest.fail("historical-only query reached Redis"),
    )

    assert reader.query_audit_log(
        "acme", end_ms=historical_end, source="auto",
    ) == []
    assert calls == [("parquet", "acme", None, historical_end, 500)]


def test_query_applies_filters_before_newest_result_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        reader,
        "_query_parquet",
        lambda *_args: [
            {"event_id": "older-match", "timestamp_ms": 10, "action": "match"},
            {"event_id": "newer-other", "timestamp_ms": 30, "action": "other"},
            {"event_id": "newer-match", "timestamp_ms": 20, "action": "match"},
        ],
    )

    result = reader.query_audit_log(
        "acme",
        source="parquet",
        action="match",
        limit=1,
    )

    assert [event["event_id"] for event in result] == ["newer-match"]


def test_auto_query_uses_complete_parquet_system_of_record_for_recent_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple] = []
    monkeypatch.setattr(
        reader,
        "_query_parquet",
        lambda org, start, end, limit: calls.append(
            (org, start, end, limit)
        ) or [{"event_id": "durable", "timestamp_ms": 20}],
    )
    monkeypatch.setattr(
        reader,
        "_query_redis",
        lambda *_args: pytest.fail("complete auto query reached best-effort Redis"),
    )

    result = reader.query_audit_log(
        "acme", source="auto", start_ms=10, end_ms=30,
    )

    assert [event["event_id"] for event in result] == ["durable"]
    assert calls == [("acme", 10, 30, 500)]


def test_hot_query_scan_ceiling_fails_closed_without_partial_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import supertable.audit.writer_redis as writer_redis

    class EndlessWriter:
        def __init__(self, *_args, **_kwargs):
            pass

        def query(self, **_kwargs):
            return [{
                "event_id": "event",
                "timestamp_ms": "1",
                "_stream_id": "1-0",
            }]

    monkeypatch.setattr(reader, "_MAX_AUDIT_QUERY_SCAN_EVENTS", 1)
    monkeypatch.setattr(writer_redis, "RedisAuditWriter", EndlessWriter)
    monkeypatch.setitem(
        sys.modules,
        "supertable.redis_infra",
        SimpleNamespace(redis_client=object()),
    )

    with pytest.raises(reader.AuditQueryError):
        reader.query_audit_log("acme", source="redis", limit=1)
