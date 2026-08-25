"""Focused scheduling and failure-policy tests for the privileged worker."""

from __future__ import annotations

import logging
import signal
import traceback
from types import SimpleNamespace

import fakeredis
import pytest

import supertable.audit.privileged_worker as worker_module
from supertable import redis_keys as RK
from supertable.audit.privileged_outbox import (
    ArchiveVerificationError,
    DeliveryPendingError,
    OutboxBackendError,
    OutboxRecordError,
)
from supertable.audit.privileged_worker import (
    ActivationBaselineError,
    ActivationBaselineReport,
    PrivilegedArchiveWorker,
    RedisDurabilityError,
    WorkerConfig,
    WorkerExitCode,
    attest_activation_baseline,
    build_parser,
    compute_privileged_state_sha256,
    main,
    verify_redis_durability,
    verify_activation_baseline,
)


def _result(*stream_ids: str, acknowledged: int | None = None):
    return SimpleNamespace(
        batch_id="a" * 64,
        stream_ids=tuple(stream_ids),
        archive={"path": "archive.parquet"},
        acknowledged=(len(stream_ids) if acknowledged is None else acknowledged),
        reused=False,
    )


class FakeOutbox:
    def __init__(
        self,
        *,
        drains=(),
        trims=(),
        health=None,
        checkpoint=None,
        chain=None,
        redis=None,
    ):
        self._drains = list(drains)
        self._trims = list(trims)
        self._health = health or SimpleNamespace(
            reachable=True,
            stream_exists=True,
            stream_length=3,
            group_count=1,
        )
        self._checkpoint = checkpoint
        self._chain = chain or {
            "organization": "org",
            "batch_count": 0,
            "first_sequence": 0,
            "last_sequence": 0,
            "latest_batch_id": None,
        }
        self._redis = redis
        self.stream_key = "supertable:org:system:audit:privileged:outbox"
        self.delivery_ledger_key = "supertable:org:system:audit:privileged:delivery"
        self.health_calls = 0
        self.checkpoint_calls = []
        self.chain_calls = []
        self.drain_calls = []
        self.trim_calls = []

    @staticmethod
    def _take(items, default):
        item = items.pop(0) if items else default
        if isinstance(item, BaseException):
            raise item
        return item

    def health(self):
        self.health_calls += 1
        if isinstance(self._health, BaseException):
            raise self._health
        return self._health

    def verify_checkpoint_head(self, organization):
        self.checkpoint_calls.append(organization)
        if isinstance(self._checkpoint, BaseException):
            raise self._checkpoint
        return self._checkpoint

    def verify_checkpoint_chain(self, organization, *, max_batches=10_000):
        self.chain_calls.append((organization, max_batches))
        if isinstance(self._chain, BaseException):
            raise self._chain
        return self._chain

    def drain_once(self, organization, **kwargs):
        self.drain_calls.append((organization, kwargs))
        return self._take(self._drains, None)

    def trim_delivered(self, group, **kwargs):
        self.trim_calls.append((group, kwargs))
        return self._take(self._trims, 0)


class FakeRedis:
    def __init__(self, *, config=None, types=None, connected=0, config_error=None):
        self.config = {
            "appendonly": "yes",
            "appendfsync": "always",
            "maxmemory-policy": "noeviction",
            "min-replicas-to-write": "0",
            "min-replicas-max-lag": "10",
            **(config or {}),
        }
        self.types = types or {}
        self.connected = connected
        self.config_error = config_error
        self.config_calls = []

    def type(self, key):
        return self.types.get(key, "none")

    def config_get(self, name):
        self.config_calls.append(name)
        if self.config_error is not None:
            raise self.config_error
        return {name: self.config[name]} if name in self.config else {}

    def info(self, section):
        assert section == "replication"
        return {"connected_slaves": self.connected}


def _config(**overrides):
    values = {
        "organization": "org",
        "consumer": "worker-1",
        "once": True,
        # Unit tests below isolate worker scheduling. Operational CLI startup
        # remains strict by default and has dedicated attestation tests.
        "require_durable_redis": False,
    }
    values.update(overrides)
    return WorkerConfig(**values)


def _baseline_args():
    return [
        "--activation-baseline",
        "baseline.json",
        "--activation-baseline-sha256",
        "a" * 64,
    ]


def _baseline_report(*_args, **_kwargs):
    return SimpleNamespace(
        organization="org",
        activation_id="activation-1",
        created_ms=1_700_000_000_000,
        state_sha256="b" * 64,
        artifact_sha256="a" * 64,
    )


def _activation_attestation(*_args, **_kwargs):
    return False


def test_once_archives_one_unit_without_implicit_trim():
    outbox = FakeOutbox(drains=[_result("1-0", "2-0")])
    worker = PrivilegedArchiveWorker(outbox, _config())

    assert worker.run() == WorkerExitCode.OK
    assert outbox.health_calls == 1
    assert outbox.checkpoint_calls == ["org"]
    assert outbox.drain_calls == [
        (
            "org",
            {
                "group": "__privileged_archival__",
                "consumer": "worker-1",
                "count": 100,
                "reclaim_idle_ms": 300_000,
            },
        )
    ]
    assert outbox.trim_calls == []
    assert worker.stats.archived_batches == 1
    assert worker.stats.archived_events == 2
    assert worker.stats.acknowledged_events == 2


def test_recoverable_headless_first_claim_does_not_block_worker_drain():
    outbox = FakeOutbox(
        checkpoint=DeliveryPendingError("first batch is durably claimed"),
        drains=[_result("1-0")],
    )
    worker = PrivilegedArchiveWorker(outbox, _config())

    assert worker.run() == WorkerExitCode.OK
    assert outbox.checkpoint_calls == ["org"]
    assert len(outbox.drain_calls) == 1
    assert worker.stats.archived_events == 1


def test_explicit_trim_uses_only_verified_drain_watermark():
    outbox = FakeOutbox(
        drains=[_result("1-0", "2-0")],
        trims=[1],
    )
    worker = PrivilegedArchiveWorker(
        outbox,
        _config(trim=True, trim_max_entries=77),
    )

    assert worker.run() == WorkerExitCode.OK
    assert outbox.trim_calls == [
        (
            "__privileged_archival__",
            {"through_id": "2-0", "max_entries": 77},
        )
    ]
    assert worker.stats.trimmed_entries == 1


def test_trim_bound_matches_outbox_batch_bound():
    assert WorkerConfig(
        organization="org",
        consumer="worker",
        trim_max_entries=1_000,
    ).trim_max_entries == 1_000
    with pytest.raises(ValueError, match="between 1 and 1000"):
        WorkerConfig(
            organization="org",
            consumer="worker",
            trim_max_entries=1_001,
        )


def test_failed_trim_retries_same_watermark_without_redraining():
    outbox = FakeOutbox(
        drains=[_result("1-0")],
        trims=[OutboxBackendError("trim", ConnectionError("down")), 0],
    )
    waits = []
    worker = PrivilegedArchiveWorker(
        outbox,
        _config(trim=True, max_retries=1, retry_jitter=0),
        wait_for_stop=lambda delay: waits.append(delay) or False,
    )

    assert worker.run() == WorkerExitCode.OK
    assert len(outbox.drain_calls) == 1
    assert len(outbox.trim_calls) == 2
    assert waits == [1.0]


def test_transient_failure_exhaustion_is_nonzero_and_bounded():
    failures = [
        OutboxBackendError("drain", ConnectionError(f"down-{index}"))
        for index in range(3)
    ]
    outbox = FakeOutbox(drains=failures)
    waits = []
    worker = PrivilegedArchiveWorker(
        outbox,
        _config(max_retries=2, retry_jitter=0),
        wait_for_stop=lambda delay: waits.append(delay) or False,
    )

    assert worker.run() == WorkerExitCode.RETRY_EXHAUSTED
    assert len(outbox.drain_calls) == 3
    assert waits == [1.0, 2.0]


def test_delivery_pending_is_bounded_and_retryable():
    outbox = FakeOutbox(
        drains=[DeliveryPendingError("concurrent claim"), _result("1-0")]
    )
    waits = []
    worker = PrivilegedArchiveWorker(
        outbox,
        _config(max_retries=1, retry_jitter=0),
        wait_for_stop=lambda delay: waits.append(delay) or False,
    )

    assert worker.run() == WorkerExitCode.OK
    assert len(outbox.drain_calls) == 2
    assert waits == [1.0]


@pytest.mark.parametrize(
    "failure",
    [
        OutboxRecordError("ledger gap"),
        ArchiveVerificationError("archive hash mismatch"),
    ],
)
def test_integrity_failure_exits_immediately_without_retry(failure):
    outbox = FakeOutbox(drains=[failure])
    waits = []
    worker = PrivilegedArchiveWorker(
        outbox,
        _config(max_retries=8),
        wait_for_stop=lambda delay: waits.append(delay) or False,
    )

    assert worker.run() == WorkerExitCode.INTEGRITY
    assert len(outbox.drain_calls) == 1
    assert waits == []


def test_poisoned_delivery_metadata_is_not_written_to_worker_logs(caplog):
    secret = "https://admin:never-log@example.test/?access_token=never-log"
    poisoned = SimpleNamespace(
        batch_id=secret,
        stream_ids=(secret,),
        acknowledged=1,
        reused=False,
    )
    outbox = FakeOutbox(drains=[poisoned])
    worker = PrivilegedArchiveWorker(outbox, _config())
    caplog.set_level(
        logging.INFO,
        logger="supertable.audit.privileged_worker",
    )

    assert worker.run() == WorkerExitCode.INTEGRITY
    assert secret not in "\n".join(record.getMessage() for record in caplog.records)


def test_health_check_is_read_only():
    outbox = FakeOutbox(drains=[AssertionError("must not drain")])
    worker = PrivilegedArchiveWorker(
        outbox,
        _config(once=False, health_check=True),
    )

    assert worker.run() == WorkerExitCode.OK
    assert outbox.health_calls == 1
    assert outbox.checkpoint_calls == ["org"]
    assert outbox.drain_calls == []
    assert outbox.trim_calls == []


def test_verify_chain_is_read_only_and_does_not_use_routine_head_check():
    chain = {
        "organization": "org",
        "batch_count": 3,
        "first_sequence": 1,
        "last_sequence": 17,
        "latest_batch_id": "a" * 64,
    }
    outbox = FakeOutbox(
        chain=chain,
        drains=[AssertionError("must not drain")],
        trims=[AssertionError("must not trim")],
    )
    worker = PrivilegedArchiveWorker(
        outbox,
        _config(once=False, verify_chain=True),
    )

    assert worker.run() == WorkerExitCode.OK
    assert outbox.chain_calls == [("org", 10_000)]
    assert outbox.checkpoint_calls == []
    assert outbox.health_calls == 0
    assert outbox.drain_calls == []
    assert outbox.trim_calls == []
    assert worker.stats.chain_verifications == 1


@pytest.mark.parametrize(
    "failure",
    [
        ArchiveVerificationError("checkpoint chain is broken"),
        OutboxRecordError("checkpoint manifest is malformed"),
        ValueError("checkpoint evidence has invalid bounds"),
    ],
)
def test_verify_chain_integrity_failure_exits_three_without_drain(failure):
    outbox = FakeOutbox(
        chain=failure,
        drains=[AssertionError("must not drain")],
    )
    worker = PrivilegedArchiveWorker(
        outbox,
        _config(once=False, verify_chain=True),
    )

    assert worker.run() == WorkerExitCode.INTEGRITY
    assert outbox.chain_calls == [("org", 10_000)]
    assert outbox.drain_calls == []


def test_verify_chain_rejects_an_inconsistent_success_summary():
    outbox = FakeOutbox(
        chain={
            "organization": "org",
            "batch_count": 1,
            "first_sequence": 1,
            "last_sequence": 5,
            "latest_batch_id": None,
        }
    )
    worker = PrivilegedArchiveWorker(
        outbox,
        _config(once=False, verify_chain=True),
    )

    assert worker.run() == WorkerExitCode.INTEGRITY
    assert outbox.drain_calls == []


def test_continuous_idle_wait_is_cooperatively_stoppable():
    outbox = FakeOutbox(drains=[None])
    waits = []
    worker = PrivilegedArchiveWorker(
        outbox,
        _config(once=False),
        wait_for_stop=lambda delay: waits.append(delay) or True,
    )

    assert worker.run() == WorkerExitCode.OK
    assert len(outbox.drain_calls) == 1
    assert waits == [1.0]


def test_continuous_worker_reverifies_checkpoint_on_heartbeat():
    outbox = FakeOutbox(drains=[None, None])
    monotonic_values = iter([0.0, 0.0, 2.0, 2.0])
    waits = iter([False, True])
    worker = PrivilegedArchiveWorker(
        outbox,
        _config(once=False, heartbeat_seconds=1.0),
        monotonic=lambda: next(monotonic_values),
        wait_for_stop=lambda _delay: next(waits),
    )

    assert worker.run() == WorkerExitCode.OK
    assert outbox.checkpoint_calls == ["org", "org"]
    assert outbox.health_calls == 2


def test_strict_redis_preflight_accepts_documented_durability_policy():
    redis = FakeRedis(
        config={"min-replicas-to-write": "2"},
        connected=2,
    )
    outbox = FakeOutbox(redis=redis)
    redis.types = {
        outbox.stream_key: b"stream",
        outbox.delivery_ledger_key: b"hash",
    }

    report = verify_redis_durability(
        outbox,
        min_replicas_to_write=2,
        min_connected_replicas=2,
    )

    assert report.appendonly == "yes"
    assert report.appendfsync == "always"
    assert report.maxmemory_policy == "noeviction"
    assert report.configured_min_replicas == 2
    assert report.connected_replicas == 2


def test_strict_redis_preflight_reports_config_acl_failure_precisely():
    outbox = FakeOutbox(redis=FakeRedis(config_error=PermissionError("NOPERM")))

    with pytest.raises(RedisDurabilityError, match="CONFIG GET ACL permission"):
        verify_redis_durability(outbox)


def test_strict_redis_preflight_redacts_hostile_backend_failure():
    secret = "redis://admin:never-log@example.test/0?access_token=never-log"
    outbox = FakeOutbox(
        redis=FakeRedis(),
        health=RuntimeError(secret),
    )

    with pytest.raises(RedisDurabilityError) as caught:
        verify_redis_durability(outbox)

    rendered = "".join(
        traceback.format_exception(
            type(caught.value), caught.value, caught.value.__traceback__,
        )
    )
    assert "error_type=RuntimeError" in str(caught.value)
    assert caught.value.__suppress_context__ is True
    assert secret not in str(caught.value)
    assert secret not in rendered


def test_everysec_requires_explicit_weaker_durability_opt_in():
    outbox = FakeOutbox(redis=FakeRedis(config={"appendfsync": "everysec"}))

    with pytest.raises(RedisDurabilityError, match="explicit everysec risk opt-in"):
        verify_redis_durability(outbox)

    report = verify_redis_durability(outbox, allow_everysec=True)
    assert report.appendfsync == "everysec"


def test_wrong_redis_key_type_fails_before_outbox_health_commands():
    redis = FakeRedis()
    outbox = FakeOutbox(redis=redis)
    redis.types[outbox.delivery_ledger_key] = "string"

    with pytest.raises(RedisDurabilityError, match="unexpected Redis type"):
        verify_redis_durability(outbox)

    assert outbox.health_calls == 0


@pytest.mark.parametrize(
    ("config", "match"),
    [
        ({"appendonly": "no"}, "appendonly must be yes"),
        ({"appendfsync": "no"}, "appendfsync must be always"),
        ({"maxmemory-policy": "allkeys-lru"}, "must be noeviction"),
    ],
)
def test_strict_redis_preflight_rejects_unsafe_settings(config, match):
    outbox = FakeOutbox(redis=FakeRedis(config=config))

    with pytest.raises(RedisDurabilityError, match=match):
        verify_redis_durability(outbox)


def test_pinned_activation_baseline_is_canonical_and_immutable(tmp_path):
    import hashlib
    import json

    document = {
        "version": 1,
        "kind": "supertable_privileged_activation_baseline",
        "organization": "org",
        "activation_id": "cutover-2026-08-18",
        "created_ms": 1_700_000_000_000,
        "state_sha256": "b" * 64,
    }
    payload = json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode()
    path = tmp_path / "baseline.json"
    path.write_bytes(payload)
    pin = hashlib.sha256(payload).hexdigest()

    report = verify_activation_baseline(
        str(path), expected_sha256=pin, organization="org",
    )
    assert report.activation_id == "cutover-2026-08-18"
    path.write_bytes(payload.replace(b"cutover", b"changed", 1))
    with pytest.raises(ActivationBaselineError, match="deployment-pinned"):
        verify_activation_baseline(
            str(path), expected_sha256=pin, organization="org",
        )


def test_activation_baseline_rejects_unpinned_schema_extensions(tmp_path):
    import hashlib
    import json

    document = {
        "version": 1,
        "kind": "supertable_privileged_activation_baseline",
        "organization": "org",
        "activation_id": "cutover",
        "created_ms": 1_700_000_000_000,
        "state_sha256": "b" * 64,
        "mutable_note": "must not become part of the trust schema",
    }
    payload = json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode()
    path = tmp_path / "baseline.json"
    path.write_bytes(payload)

    with pytest.raises(ActivationBaselineError, match="contain exactly"):
        verify_activation_baseline(
            str(path),
            expected_sha256=hashlib.sha256(payload).hexdigest(),
            organization="org",
        )


def _activation_outbox(redis_client):
    return FakeOutbox(redis=redis_client)


def test_live_privileged_state_digest_is_canonical_and_covers_all_namespaces():
    redis_client = fakeredis.FakeStrictRedis(decode_responses=True)
    outbox = _activation_outbox(redis_client)
    role_key = RK.rbac_role_doc("org", "lake", "reader")
    redis_client.hset(role_key, mapping={"z": "last", "a": "first"})
    redis_client.sadd(RK.rbac_role_index("org", "lake"), "reader")
    redis_client.hset(RK.auth_tokens("org"), "token-1", "secret-digest")

    first = compute_privileged_state_sha256(outbox, "org")
    # Rewriting in a different Redis insertion order cannot move the digest.
    redis_client.delete(role_key)
    redis_client.hset(role_key, mapping={"a": "first", "z": "last"})
    assert compute_privileged_state_sha256(outbox, "org") == first

    redis_client.hset(role_key, "z", "changed")
    assert compute_privileged_state_sha256(outbox, "org") != first


def test_activation_anchor_matches_live_state_once_then_remains_genesis():
    redis_client = fakeredis.FakeStrictRedis(decode_responses=True)
    outbox = _activation_outbox(redis_client)
    redis_client.hset(
        RK.rbac_role_doc("org", "lake", "reader"),
        mapping={"role_id": "reader", "doc_version": "1"},
    )
    report = ActivationBaselineReport(
        organization="org",
        activation_id="cutover-1",
        created_ms=1_700_000_000_000,
        state_sha256=compute_privileged_state_sha256(outbox, "org"),
        artifact_sha256="a" * 64,
    )

    assert attest_activation_baseline(outbox, report) is True
    # Normal audited state evolution does not rewrite the immutable genesis.
    redis_client.hset(
        RK.rbac_role_doc("org", "lake", "reader"), "doc_version", "2"
    )
    assert attest_activation_baseline(outbox, report) is False

    changed_report = ActivationBaselineReport(
        organization="org",
        activation_id="different-cutover",
        created_ms=report.created_ms,
        state_sha256=report.state_sha256,
        artifact_sha256="b" * 64,
    )
    with pytest.raises(ActivationBaselineError, match="differs from baseline"):
        attest_activation_baseline(outbox, changed_report)


def test_activation_rejects_arbitrary_state_hash_and_preexisting_ledger():
    redis_client = fakeredis.FakeStrictRedis(decode_responses=True)
    outbox = _activation_outbox(redis_client)
    bad_report = ActivationBaselineReport(
        organization="org",
        activation_id="cutover-bad",
        created_ms=1_700_000_000_000,
        state_sha256="f" * 64,
        artifact_sha256="a" * 64,
    )
    with pytest.raises(ActivationBaselineError, match="does not match live"):
        attest_activation_baseline(outbox, bad_report)
    assert not redis_client.exists(RK.audit_privileged_activation("org"))

    valid_report = ActivationBaselineReport(
        organization="org",
        activation_id="cutover-valid",
        created_ms=1_700_000_000_000,
        state_sha256=compute_privileged_state_sha256(outbox, "org"),
        artifact_sha256="a" * 64,
    )
    redis_client.xadd(
        RK.audit_privileged_outbox("org"), {"event_json": "legacy-event"}
    )
    with pytest.raises(ActivationBaselineError, match="ledger exists"):
        attest_activation_baseline(outbox, valid_report)


def test_worker_reattests_after_transparent_redis_reconnect():
    first_redis = object()
    second_redis = object()
    outbox = FakeOutbox(
        redis=first_redis,
        drains=[_result("1-0"), None],
    )
    seen = []

    def checker(candidate, **_kwargs):
        seen.append(candidate._redis)
        if len(seen) == 1:
            candidate._redis = second_redis
        return SimpleNamespace()

    worker = PrivilegedArchiveWorker(
        outbox,
        _config(once=False, require_durable_redis=True),
        durability_checker=checker,
        wait_for_stop=lambda _delay: True,
    )
    assert worker.run() == WorkerExitCode.OK
    assert seen == [first_redis, second_redis]
    assert worker.stats.redis_attestations == 2


def test_worker_rechecks_baseline_each_unit_and_fails_closed_on_change():
    outbox = FakeOutbox(drains=[_result("1-0"), None])
    calls = []

    def checker(*_args, **_kwargs):
        calls.append(1)
        if len(calls) == 2:
            raise ActivationBaselineError("baseline replaced")
        return _baseline_report()

    worker = PrivilegedArchiveWorker(
        outbox,
        _config(
            once=False,
            activation_baseline_path="baseline.json",
            activation_baseline_sha256="a" * 64,
        ),
        baseline_checker=checker,
        activation_checker=_activation_attestation,
    )
    assert worker.run() == WorkerExitCode.CONFIG
    assert len(outbox.drain_calls) == 1
    assert calls == [1, 1]


def test_operational_main_requires_pinned_activation_baseline():
    created = []
    code = main(
        ["--organization", "org", "--consumer", "worker", "--health-check"],
        outbox_factory=lambda _organization: created.append(True),
    )
    assert code == WorkerExitCode.CONFIG
    assert created == []


def test_cli_bounds_batch_size_and_keeps_trim_opt_in():
    parser = build_parser()
    args = parser.parse_args(["--organization", "org", "--consumer", "worker"])
    assert args.trim is False
    assert args.verify_max_batches == 10_000
    assert args.require_durable_redis is True

    with pytest.raises(SystemExit) as raised:
        main(
            [
                "--organization",
                "org",
                "--consumer",
                "worker",
                "--count",
                "1001",
            ],
            outbox_factory=lambda _organization: FakeOutbox(),
        )
    assert raised.value.code == WorkerExitCode.CONFIG


def test_cli_configuration_failure_never_echoes_exception_message(
    monkeypatch,
    capsys,
):
    secret = "https://admin:never-log@example.test/?access_token=never-log"

    def fail_config(_args):
        raise ValueError(secret)

    monkeypatch.setattr(worker_module, "_config_from_args", fail_config)
    with pytest.raises(SystemExit) as raised:
        main(["--organization", "org", "--consumer", "worker"])

    assert raised.value.code == WorkerExitCode.CONFIG
    assert secret not in capsys.readouterr().err


def test_verify_chain_cli_mode_is_exclusive_and_rejects_trim():
    with pytest.raises(SystemExit) as mutually_exclusive:
        build_parser().parse_args(
            [
                "--organization",
                "org",
                "--consumer",
                "worker",
                "--once",
                "--verify-chain",
            ]
        )
    assert mutually_exclusive.value.code == WorkerExitCode.CONFIG

    with pytest.raises(ValueError, match="cannot be combined with trim"):
        _config(once=False, verify_chain=True, trim=True)


@pytest.mark.parametrize("value", [True, 0, 1_000_001])
def test_verify_chain_batch_bound_rejects_noncanonical_or_unbounded_values(value):
    with pytest.raises(ValueError, match="verify_max_batches must be between"):
        _config(verify_max_batches=value)


def test_main_verify_chain_mode_passes_explicit_bounded_walk_limit():
    outbox = FakeOutbox(drains=[AssertionError("must not drain")])

    code = main(
        [
            "--organization",
            "org",
            "--consumer",
            "verifier",
            "--verify-chain",
            "--verify-max-batches",
            "250000",
            "--no-require-durable-redis",
            *_baseline_args(),
        ],
        outbox_factory=lambda _organization: outbox,
        baseline_checker=_baseline_report,
        activation_checker=_activation_attestation,
    )

    assert code == WorkerExitCode.OK
    assert outbox.chain_calls == [("org", 250_000)]
    assert outbox.checkpoint_calls == []
    assert outbox.health_calls == 0
    assert outbox.drain_calls == []
    assert outbox.trim_calls == []


def test_main_explicit_unsafe_opt_out_does_not_request_redis_config():
    outbox = FakeOutbox(redis=FakeRedis(config_error=AssertionError("CONFIG called")))

    code = main(
        [
            "--organization",
            "org",
            "--consumer",
            "probe",
            "--health-check",
            "--no-require-durable-redis",
            *_baseline_args(),
        ],
        outbox_factory=lambda _organization: outbox,
        baseline_checker=_baseline_report,
        activation_checker=_activation_attestation,
    )

    assert code == WorkerExitCode.OK
    assert outbox._redis.config_calls == []


def test_main_passes_explicit_everysec_opt_in_to_strict_checker():
    outbox = FakeOutbox()
    seen = {}

    def checker(candidate, **kwargs):
        seen["candidate"] = candidate
        seen.update(kwargs)
        return SimpleNamespace(
            stream_type="none",
            delivery_type="none",
            appendonly="yes",
            appendfsync="everysec",
            maxmemory_policy="noeviction",
            configured_min_replicas=0,
            connected_replicas=0,
        )

    code = main(
        [
            "--organization",
            "org",
            "--consumer",
            "probe",
            "--health-check",
            "--require-durable-redis",
            "--allow-everysec",
            *_baseline_args(),
        ],
        outbox_factory=lambda _organization: outbox,
        durability_checker=checker,
        baseline_checker=_baseline_report,
        activation_checker=_activation_attestation,
    )

    assert code == WorkerExitCode.OK
    assert seen == {
        "candidate": outbox,
        "allow_everysec": True,
        "min_replicas_to_write": 0,
        "min_connected_replicas": 0,
    }


def test_strict_preflight_failure_exits_before_checkpoint_or_drain():
    outbox = FakeOutbox(drains=[AssertionError("must not drain")])

    code = main(
        [
            "--organization",
            "org",
            "--consumer",
            "worker",
            "--once",
            "--require-durable-redis",
            *_baseline_args(),
        ],
        outbox_factory=lambda _organization: outbox,
        durability_checker=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RedisDurabilityError("appendonly must be yes")
        ),
        baseline_checker=_baseline_report,
        activation_checker=_activation_attestation,
    )

    assert code == WorkerExitCode.CONFIG
    assert outbox.checkpoint_calls == []
    assert outbox.drain_calls == []


def test_checkpoint_integrity_failure_exits_before_drain():
    outbox = FakeOutbox(
        checkpoint=ArchiveVerificationError("checkpoint hash mismatch"),
        drains=[AssertionError("must not drain")],
    )
    worker = PrivilegedArchiveWorker(outbox, _config())

    assert worker.run() == WorkerExitCode.INTEGRITY
    assert outbox.checkpoint_calls == ["org"]
    assert outbox.health_calls == 0
    assert outbox.drain_calls == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("count", 1.5),
        ("reclaim_idle_ms", "0"),
        ("max_retries", 1.0),
        ("poll_seconds", float("inf")),
        ("retry_jitter", float("nan")),
        ("trim", 1),
    ],
)
def test_worker_config_rejects_noncanonical_numeric_and_boolean_types(field, value):
    with pytest.raises(ValueError):
        _config(**{field: value})


def test_signal_request_marks_worker_for_clean_exit():
    worker = PrivilegedArchiveWorker(FakeOutbox(), _config(once=False))

    worker.request_stop(15)

    assert worker.stop_event.is_set()
    assert worker.run() == WorkerExitCode.OK


def test_signal_handlers_request_shutdown_and_restore_previous_handlers(monkeypatch):
    worker = PrivilegedArchiveWorker(FakeOutbox(), _config(once=False))
    installed = {}
    previous_handlers = {
        signal.SIGTERM: object(),
        signal.SIGINT: object(),
    }

    monkeypatch.setattr(
        worker_module.signal,
        "getsignal",
        lambda signum: previous_handlers[signum],
    )
    monkeypatch.setattr(
        worker_module.signal,
        "signal",
        lambda signum, handler: installed.__setitem__(signum, handler),
    )

    previous = worker_module._install_signal_handlers(worker)
    installed[signal.SIGTERM](signal.SIGTERM, None)

    assert previous == previous_handlers
    assert worker.stop_event.is_set()

    worker_module._restore_signal_handlers(previous)
    assert installed == previous_handlers
