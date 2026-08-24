from __future__ import annotations

import json

import fakeredis
import pytest

from supertable.quality.config import (
    BUILTIN_CHECKS,
    DQConfig,
    DQConfigConflictError,
    DQConfigReadError,
)
from supertable import redis_keys as RK


def _seed_live_catalog(redis_client, table="facts"):
    redis_client.set(
        RK.meta_root("org", "lake"),
        json.dumps({"version": 0, "ts": 1}),
    )
    redis_client.set(
        RK.meta_leaf("org", "lake", table),
        json.dumps({
            "version": 0,
            "ts": 1,
            "path": f"org/lake/{table}/snapshot.json",
        }),
    )


def _config():
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    _seed_live_catalog(redis_client)
    return redis_client, DQConfig(redis_client, "org", "lake")


def test_old_partial_global_config_is_forward_merged_with_all_builtins():
    redis_client, config = _config()
    redis_client.set(
        config._key("config", "__global__"),
        json.dumps({
            "checks": {
                "T1": {"enabled": False, "threshold": 99},
                "UNKNOWN": {"enabled": True},
            },
        }),
    )
    result = config.get_global_config()
    assert set(result["checks"]) == set(BUILTIN_CHECKS)
    assert result["checks"]["T1"] == {"enabled": False, "threshold": 99}
    assert result["checks"]["T2"]["enabled"] is True


def test_table_overrides_do_not_remove_unmentioned_checks():
    _, config = _config()
    assert config.set_table_config("facts", {"checks": {"D7": {"enabled": True}}})
    effective = config.get_effective_config("facts")
    assert set(effective["checks"]) == set(BUILTIN_CHECKS)
    assert effective["checks"]["D7"]["enabled"] is True
    assert effective["checks"]["T1"]["enabled"] is True


def test_config_setters_do_not_mutate_the_callers_object():
    _, config = _config()
    global_document = {"checks": {"T1": {"enabled": False}}}
    table_document = {"scope": "full"}
    assert config.set_global_config(global_document, "alice")
    assert config.set_table_config("facts", table_document, "alice")
    assert global_document == {"checks": {"T1": {"enabled": False}}}
    assert table_document == {"scope": "full"}


def test_schedule_cooldown_rejects_values_outside_redis_safe_range():
    redis_client, config = _config()
    too_large = (1 << 31)

    assert not config.set_schedule({"cooldown_seconds": too_large})
    assert not config.set_table_schedule(
        "facts", {"cooldown_seconds": too_large},
    )
    assert redis_client.get(config._key("schedule")) is None
    assert redis_client.get(config._key("schedule", "facts")) is None

    redis_client.set(
        config._key("schedule"),
        json.dumps({"cooldown_seconds": too_large}),
    )
    with pytest.raises(DQConfigReadError, match="cooldown_seconds"):
        config.get_schedule()


@pytest.mark.parametrize("column", ["event_time", "__timestamp__", None])
def test_new_incremental_config_is_rejected_for_every_cursor(column):
    redis_client, config = _config()
    document = {
        "scope": "incremental",
    }
    if column is not None:
        document["incremental_column"] = column

    assert not config.set_global_config(document)
    assert not config.set_table_config("facts", document)
    assert redis_client.get(config._key("config", "__global__")) is None
    assert redis_client.get(config._key("config", "facts")) is None


@pytest.mark.parametrize("column", ["event_time", "__timestamp__"])
def test_legacy_incremental_config_degrades_to_full(column):
    redis_client, config = _config()
    redis_client.set(
        config._key("config", "facts"),
        json.dumps({
            "scope": "incremental",
            "incremental_column": column,
        }),
    )
    table_config = config.get_table_config("facts")
    effective = config.get_effective_config("facts")
    assert table_config["scope"] == "full"
    assert "incremental_column" not in table_config
    assert effective["scope"] == "full"
    assert "incremental_column" not in effective


def test_legacy_global_incremental_config_degrades_to_full():
    redis_client, config = _config()
    redis_client.set(
        config._key("config", "__global__"),
        json.dumps({
            "scope": "incremental",
            "incremental_column": "event_time",
        }),
    )

    global_config = config.get_global_config()
    assert global_config["scope"] == "full"
    assert "incremental_column" not in global_config


def test_custom_rule_ingress_rejects_hidden_columns_without_mutating_input():
    redis_client, config = _config()
    document = {
        "table_name": "facts",
        "rule_type": "column_min",
        "column_name": "__rowid__",
        "threshold": 0,
    }
    original = dict(document)
    with pytest.raises(ValueError, match="hidden_column"):
        config.create_rule(document)
    assert document == original
    assert redis_client.scard(config._key("rules", "index")) == 0


def test_custom_rule_ingress_rejects_malformed_shape():
    _, config = _config()
    with pytest.raises(ValueError, match="table_name"):
        config.create_rule({
            "rule_type": "row_count_min",
            "threshold": 10,
        })
    with pytest.raises(ValueError, match="enabled"):
        config.create_rule({
            "table_name": "facts",
            "rule_type": "row_count_min",
            "threshold": 10,
            "enabled": "false",
        })


@pytest.mark.parametrize(
    "table_name",
    [
        "",
        "facts; DROP TABLE lake.other",
        "lake.other",
        "two words",
        "facts-name",
        "__data_quality__",
        "lake",
    ],
)
def test_custom_rule_ingress_rejects_unsafe_or_system_table_scopes(table_name):
    redis_client, config = _config()
    with pytest.raises(ValueError, match="table_name|system_table"):
        config.create_rule({
            "table_name": table_name,
            "rule_type": "row_count_min",
            "threshold": 1,
        })
    assert redis_client.scard(config._key("rules", "index")) == 0


def test_structured_rule_may_explicitly_use_wildcard_scope():
    _, config = _config()
    created = config.create_rule({
        "table_name": "*",
        "rule_type": "row_count_min",
        "threshold": 1,
    })
    assert created["table_name"] == "*"


@pytest.mark.parametrize(
    ("expected_values", "error_code"),
    [
        (list(range(257)), "expected_values_count"),
        (["x" * (16 * 1024)], "expected_values_size"),
    ],
)
def test_distinct_in_ingress_rejects_oversized_expected_values(
    expected_values, error_code,
):
    redis_client, config = _config()
    with pytest.raises(ValueError, match=error_code):
        config.create_rule({
            "table_name": "facts",
            "rule_type": "distinct_in",
            "column_name": "label",
            "expected_values": expected_values,
        })
    assert redis_client.scard(config._key("rules", "index")) == 0


def test_invalid_legacy_rule_can_be_disabled_but_not_reenabled():
    redis_client, config = _config()
    redis_client.set(
        config._key("rules", "doc", "legacy"),
        json.dumps({
            "rule_id": "legacy",
            "table_name": "facts",
            "rule_type": "column_min",
            "column_name": "__timestamp__",
            "threshold": 0,
            "enabled": True,
        }),
    )
    redis_client.sadd(config._key("rules", "index"), "legacy")
    disabled = config.update_rule("legacy", {"enabled": False})
    assert disabled["enabled"] is False
    with pytest.raises(ValueError, match="hidden_column"):
        config.update_rule("legacy", {"enabled": True})


def test_custom_sql_ingress_is_confined_to_its_attached_table():
    _, config = _config()
    with pytest.raises(ValueError, match="table_scope"):
        config.create_rule({
            "table_name": "facts",
            "rule_type": "custom_sql",
            "sql": "SELECT COUNT(*) FROM lake.other",
        })
    with pytest.raises(ValueError, match="system_table"):
        config.create_rule({
            "table_name": "__data_quality__",
            "rule_type": "custom_sql",
            "sql": "SELECT COUNT(*) FROM lake.__data_quality__",
        })
    with pytest.raises(ValueError, match="wildcard"):
        config.create_rule({
            "table_name": "*",
            "rule_type": "custom_sql",
            "sql": "SELECT COUNT(*) FROM lake.facts",
        })


def test_custom_sql_ingress_rejects_cte_even_over_attached_table():
    _, config = _config()
    with pytest.raises(ValueError, match="cte_query"):
        config.create_rule({
            "table_name": "facts",
            "rule_type": "custom_sql",
            "sql": (
                "WITH source AS (SELECT amount FROM lake.facts) "
                "SELECT MAX(amount) FROM source"
            ),
        })


def test_custom_sql_ingress_accepts_bounded_aggregate_of_attached_table():
    _, config = _config()
    created = config.create_rule({
        "table_name": "facts",
        "rule_type": "custom_sql",
        "sql": "SELECT COUNT(*) AS violations FROM lake.facts WHERE amount < -100",
    })
    assert created["enabled"] is True


def _valid_rule(rule_id="persisted"):
    return {
        "rule_id": rule_id,
        "table_name": "facts",
        "rule_type": "row_count_min",
        "threshold": 1,
    }


class _FaultPipeline:
    def __init__(self, inner, phase):
        self.inner = inner
        self.phase = phase

    def watch(self, *args, **kwargs):
        self.inner.watch(*args, **kwargs)
        return self

    def exists(self, *args, **kwargs):
        return self.inner.exists(*args, **kwargs)

    def get(self, *args, **kwargs):
        return self.inner.get(*args, **kwargs)

    def sismember(self, *args, **kwargs):
        return self.inner.sismember(*args, **kwargs)

    def type(self, *args, **kwargs):
        return self.inner.type(*args, **kwargs)

    def multi(self):
        self.inner.multi()
        return self

    def set(self, *args, **kwargs):
        if self.phase == "set":
            raise RuntimeError("injected SET failure")
        self.inner.set(*args, **kwargs)
        return self

    def sadd(self, *args, **kwargs):
        if self.phase == "sadd":
            raise RuntimeError("injected SADD failure")
        self.inner.sadd(*args, **kwargs)
        return self

    def delete(self, *args, **kwargs):
        if self.phase == "delete":
            raise RuntimeError("injected DEL failure")
        self.inner.delete(*args, **kwargs)
        return self

    def srem(self, *args, **kwargs):
        if self.phase == "srem":
            raise RuntimeError("injected SREM failure")
        self.inner.srem(*args, **kwargs)
        return self

    def execute(self):
        if self.phase == "exec":
            raise RuntimeError("injected EXEC failure")
        if self.phase == "exec_after_commit":
            self.inner.execute()
            raise RuntimeError("injected lost EXEC acknowledgement")
        if self.phase == "invalid_result":
            return [False, 0]
        return self.inner.execute()

    def reset(self):
        self.inner.reset()


class _FaultRedis:
    def __init__(self, inner, phase):
        self.inner = inner
        self.phase = phase

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def pipeline(self, *args, **kwargs):
        if self.phase == "pipeline":
            raise RuntimeError("injected pipeline failure")
        return _FaultPipeline(self.inner.pipeline(*args, **kwargs), self.phase)


@pytest.mark.parametrize(
    "phase",
    ["pipeline", "set", "sadd", "exec", "exec_after_commit", "invalid_result"],
)
def test_create_rule_never_reports_success_when_transaction_fails(phase):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    config = DQConfig(_FaultRedis(redis_client, phase), "org", "lake")
    document_key = config._key("rules", "doc", "persisted")
    index_key = config._key("rules", "index")

    with pytest.raises(RuntimeError, match="could not persist quality rule"):
        config.create_rule(_valid_rule())

    # A transaction may have committed before its acknowledgement was lost,
    # but it must never expose only one half of the rule.
    document_exists = redis_client.get(document_key) is not None
    indexed = bool(redis_client.sismember(index_key, "persisted"))
    assert document_exists is indexed


def test_create_rule_rejects_duplicate_caller_id_without_overwrite():
    redis_client, config = _config()
    first = config.create_rule(_valid_rule("same-id"), created_by="first")
    original = redis_client.get(config._key("rules", "doc", "same-id"))

    with pytest.raises(ValueError, match="already exists"):
        config.create_rule(
            {**_valid_rule("same-id"), "threshold": 999},
            created_by="second",
        )

    assert redis_client.get(config._key("rules", "doc", "same-id")) == original
    assert config.get_rule("same-id") == first
    assert redis_client.scard(config._key("rules", "index")) == 1


@pytest.mark.parametrize("phase", ["pipeline", "set", "exec", "invalid_result"])
def test_update_rule_never_returns_candidate_when_transaction_fails(phase):
    redis_client, healthy = _config()
    healthy.create_rule(_valid_rule("update_me"))
    document_key = healthy._key("rules", "doc", "update_me")
    original = redis_client.get(document_key)

    config = DQConfig(_FaultRedis(redis_client, phase), "org", "lake")
    with pytest.raises(RuntimeError, match="could not persist quality rule"):
        config.update_rule("update_me", {"threshold": 999})

    assert redis_client.get(document_key) == original


def test_update_rule_concurrent_delete_never_recreates_document():
    redis_client, healthy = _config()
    healthy.create_rule(_valid_rule("delete_race"))
    document_key = healthy._key("rules", "doc", "delete_race")
    index_key = healthy._key("rules", "index")

    class DeleteBetweenReadAndExecPipeline(_FaultPipeline):
        def __init__(self, inner):
            super().__init__(inner, "none")
            self.deleted = False

        def multi(self):
            self.inner.multi()
            if not self.deleted:
                self.deleted = True
                redis_client.delete(document_key)
                redis_client.srem(index_key, "delete_race")
            return self

    class DeleteRaceRedis:
        def __getattr__(self, name):
            return getattr(redis_client, name)

        def pipeline(self, *args, **kwargs):
            return DeleteBetweenReadAndExecPipeline(
                redis_client.pipeline(*args, **kwargs),
            )

    config = DQConfig(DeleteRaceRedis(), "org", "lake")
    assert config.update_rule("delete_race", {"threshold": 999}) is None
    assert redis_client.get(document_key) is None
    assert not redis_client.sismember(index_key, "delete_race")


def test_update_rule_rejects_stale_authorized_document_fingerprint():
    _, config = _config()
    original = config.create_rule(_valid_rule("stale-update"))
    expected = config.rule_fingerprint(original)
    changed = config.update_rule("stale-update", {"threshold": 7})

    with pytest.raises(DQConfigConflictError, match="changed before update"):
        config.update_rule(
            "stale-update",
            {"threshold": 999},
            expected_fingerprint=expected,
        )

    assert config.get_rule("stale-update") == changed


def test_update_rule_rejects_orphaned_document_and_dangling_index():
    redis_client, config = _config()
    config.create_rule(_valid_rule("orphan"))
    document_key = config._key("rules", "doc", "orphan")
    index_key = config._key("rules", "index")
    original = redis_client.get(document_key)

    redis_client.srem(index_key, "orphan")
    with pytest.raises(DQConfigReadError, match="not indexed"):
        config.update_rule("orphan", {"threshold": 999})
    assert redis_client.get(document_key) == original

    redis_client.sadd(index_key, "orphan")
    redis_client.delete(document_key)
    with pytest.raises(DQConfigReadError, match="is missing"):
        config.update_rule("orphan", {"threshold": 999})
    assert redis_client.get(document_key) is None
    assert redis_client.sismember(index_key, "orphan")


def test_disabling_rule_cannot_persist_inventory_poison():
    redis_client, config = _config()
    original = config.create_rule(_valid_rule("disable_safely"))
    document_key = config._key("rules", "doc", "disable_safely")

    with pytest.raises(DQConfigReadError, match="unknown rule_type"):
        config.update_rule(
            "disable_safely",
            {"enabled": False, "rule_type": "garbage"},
        )

    assert config.get_rule("disable_safely") == original
    assert config.list_rules() == [original]
    assert json.loads(redis_client.get(document_key)) == original


class _ReadFaultRedis:
    def __init__(self, inner, *, get_key=None, smembers=False):
        self.inner = inner
        self.get_key = get_key
        self.fail_smembers = smembers

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def get(self, key):
        if self.get_key is not None and key == self.get_key:
            raise RuntimeError("injected GET failure")
        return self.inner.get(key)

    def smembers(self, key):
        if self.fail_smembers:
            raise RuntimeError("injected SMEMBERS failure")
        return self.inner.smembers(key)


def test_absent_execution_config_is_distinct_from_read_failure():
    _, config = _config()

    assert set(config.get_global_config()["checks"]) == set(BUILTIN_CHECKS)
    assert config.get_table_config("facts") is None
    assert config.get_table_schedule("facts") is None
    assert config.get_latest("facts") is None
    assert config.get_latest_column("facts", "amount") is None
    assert config.get_anomalies("facts") == []
    assert config.list_rules() == []


@pytest.mark.parametrize(
    ("key_parts", "reader"),
    [
        (("config", "__global__"), lambda c: c.get_global_config()),
        (("config", "facts"), lambda c: c.get_table_config("facts")),
        (("schedule",), lambda c: c.get_schedule()),
        (("schedule", "facts"), lambda c: c.get_table_schedule("facts")),
        (("latest", "facts"), lambda c: c.get_latest("facts")),
        (("latest", "facts", "amount"), lambda c: c.get_latest_column("facts", "amount")),
        (("anomalies", "facts"), lambda c: c.get_anomalies("facts")),
    ],
)
def test_execution_reads_raise_on_backend_get_failure(key_parts, reader):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    healthy = DQConfig(redis_client, "org", "lake")
    fault = _ReadFaultRedis(redis_client, get_key=healthy._key(*key_parts))
    config = DQConfig(fault, "org", "lake")

    with pytest.raises(DQConfigReadError):
        reader(config)


@pytest.mark.parametrize(
    ("key_parts", "reader", "raw"),
    [
        (("config", "__global__"), lambda c: c.get_global_config(), "{"),
        (("config", "facts"), lambda c: c.get_table_config("facts"), "[]"),
        (("schedule",), lambda c: c.get_schedule(), "not-json"),
        (("schedule", "facts"), lambda c: c.get_table_schedule("facts"), "null"),
        (("latest", "facts"), lambda c: c.get_latest("facts"), "[]"),
        (("latest", "facts", "amount"), lambda c: c.get_latest_column("facts", "amount"), "{"),
        (("anomalies", "facts"), lambda c: c.get_anomalies("facts"), "{}"),
    ],
)
def test_execution_reads_raise_on_corrupt_persisted_json(key_parts, reader, raw):
    redis_client, config = _config()
    redis_client.set(config._key(*key_parts), raw)

    with pytest.raises(DQConfigReadError):
        reader(config)


def test_rule_inventory_read_failures_and_corruption_never_look_empty():
    redis_client, healthy = _config()
    index_key = healthy._key("rules", "index")

    smembers_fault = DQConfig(
        _ReadFaultRedis(redis_client, smembers=True), "org", "lake",
    )
    with pytest.raises(DQConfigReadError, match="index"):
        smembers_fault.list_rules_for_table("facts")

    redis_client.sadd(index_key, "broken")
    document_key = healthy._key("rules", "doc", "broken")
    get_fault = DQConfig(
        _ReadFaultRedis(redis_client, get_key=document_key), "org", "lake",
    )
    with pytest.raises(DQConfigReadError):
        get_fault.list_rules()

    # An indexed-but-missing document and malformed document are both
    # uncertain inventories, never evidence that custom checks are disabled.
    with pytest.raises(DQConfigReadError, match="missing"):
        healthy.list_rules()
    redis_client.set(document_key, "not-json")
    with pytest.raises(DQConfigReadError, match="malformed"):
        healthy.list_rules_for_table("facts")


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ({}, "identity"),
        ({
            "rule_id": "broken", "enabled": "true", "rule_type": "row_count_min",
            "table_name": "facts", "threshold": 1,
        }, "non-boolean"),
        ({
            "rule_id": "different", "enabled": True, "rule_type": "row_count_min",
            "table_name": "facts", "threshold": 1,
        }, "identity"),
        ({
            "rule_id": "broken", "enabled": True, "rule_type": "unknown",
            "table_name": "facts",
        }, "unknown"),
        ({
            "rule_id": "broken", "enabled": True, "rule_type": "column_min",
            "table_name": "facts", "column_name": "amount",
        }, "required fields"),
        ({
            "rule_id": "broken", "enabled": True, "rule_type": "row_count_min",
            "table_name": "facts;drop", "threshold": 1,
        }, "table scope"),
    ],
)
def test_rule_inventory_validates_identity_enabled_scope_and_minimum_shape(
    document, message,
):
    redis_client, config = _config()
    redis_client.sadd(config._key("rules", "index"), "broken")
    redis_client.set(
        config._key("rules", "doc", "broken"),
        json.dumps(document),
    )
    with pytest.raises(DQConfigReadError, match=message):
        config.list_rules()
    with pytest.raises(DQConfigReadError):
        config.list_rules_for_table("facts")


def test_disabled_recognisable_legacy_rule_is_listable_only_for_remediation():
    redis_client, config = _config()
    document = {
        "rule_id": "legacy_hidden",
        "enabled": False,
        "rule_type": "column_min",
        "table_name": "facts",
        "column_name": "__timestamp__",
        "threshold": 0,
    }
    redis_client.sadd(config._key("rules", "index"), document["rule_id"])
    redis_client.set(
        config._key("rules", "doc", document["rule_id"]),
        json.dumps(document),
    )
    assert config.list_rules() == [document]
    assert config.list_rules_for_table("facts") == []


@pytest.mark.parametrize(
    ("check_id", "override"),
    [
        ("T1", {"enabled": 1}),
        ("C1", {"enabled": "false"}),
        ("C2", {"threshold": float("nan")}),
        ("C5", {"threshold": float("inf")}),
        ("C6", {"threshold": float("-inf")}),
        ("T1", {"threshold": "10"}),
        ("C1", {"threshold": -1}),
        ("T3", {"threshold": 0}),
    ],
)
def test_builtin_config_setters_reject_noncanonical_values(check_id, override):
    redis_client, config = _config()
    document = {"checks": {check_id: override}}

    assert not config.set_global_config(document)
    assert not config.set_table_config("facts", document)
    assert redis_client.get(config._key("config", "__global__")) is None
    assert redis_client.get(config._key("config", "facts")) is None


@pytest.mark.parametrize(
    ("check_id", "override"),
    [
        ("T1", {"enabled": None}),
        ("C1", {"threshold": float("nan")}),
        ("C2", {"threshold": float("inf")}),
        ("C5", {"threshold": "5"}),
        ("C6", {"threshold": -0.1}),
        ("C3", {"threshold": 1}),
    ],
)
def test_invalid_persisted_builtin_config_raises_fail_closed(check_id, override):
    redis_client, config = _config()
    payload = json.dumps({"checks": {check_id: override}})
    redis_client.set(config._key("config", "__global__"), payload)

    with pytest.raises(DQConfigReadError):
        config.get_global_config()

    redis_client.delete(config._key("config", "__global__"))
    redis_client.set(config._key("config", "facts"), payload)
    with pytest.raises(DQConfigReadError):
        config.get_table_config("facts")


def test_authoritative_setters_return_false_on_false_redis_acknowledgement():
    redis_client = fakeredis.FakeRedis(decode_responses=True)

    class FalseSetRedis:
        def __getattr__(self, name):
            return getattr(redis_client, name)

        def set(self, *_args, **_kwargs):
            return False

    config = DQConfig(FalseSetRedis(), "org", "lake")
    assert not config.set_global_config({"checks": {}})
    assert not config.set_table_config("facts", {"checks": {}})
    assert not config.set_schedule({"enabled": True})
    assert not config.set_table_schedule("facts", {"enabled": True})
    assert not config.set_latest("facts", {"status": "ok"})
    assert not config.set_latest_column("facts", "amount", {"status": "ok"})
    assert not config.set_anomalies("facts", [])


@pytest.mark.parametrize("phase", ["pipeline", "delete", "srem", "exec"])
def test_delete_rule_is_atomic_and_returns_false_on_transaction_failure(phase):
    redis_client, healthy = _config()
    healthy.create_rule(_valid_rule("delete_me"))
    config = DQConfig(_FaultRedis(redis_client, phase), "org", "lake")

    assert not config.delete_rule("delete_me")
    assert redis_client.get(config._key("rules", "doc", "delete_me")) is not None
    assert redis_client.sismember(config._key("rules", "index"), "delete_me")


def test_delete_rule_wrong_type_index_cannot_partially_delete_document():
    redis_client, config = _config()
    document = _valid_rule("wrong-type-delete")
    document_key = config._key("rules", "doc", document["rule_id"])
    index_key = config._key("rules", "index")
    encoded = json.dumps(document)
    redis_client.set(document_key, encoded)
    redis_client.set(index_key, "corrupt-not-a-set")

    assert not config.delete_rule(document["rule_id"])
    assert redis_client.get(document_key) == encoded
    assert redis_client.get(index_key) == "corrupt-not-a-set"


def test_delete_rule_rejects_stale_authorized_document_fingerprint():
    redis_client, config = _config()
    original = config.create_rule(_valid_rule("stale-delete"))
    expected = config.rule_fingerprint(original)
    changed = config.update_rule("stale-delete", {"threshold": 7})

    with pytest.raises(DQConfigConflictError, match="changed before delete"):
        config.delete_rule(
            "stale-delete",
            expected_fingerprint=expected,
        )

    assert config.get_rule("stale-delete") == changed
    assert redis_client.sismember(
        config._key("rules", "index"), "stale-delete",
    )


def test_create_rule_wrong_type_index_cannot_leave_orphan_document():
    redis_client, config = _config()
    rule_id = "wrong-type-create"
    document_key = config._key("rules", "doc", rule_id)
    index_key = config._key("rules", "index")
    redis_client.set(index_key, "corrupt-not-a-set")

    with pytest.raises(RuntimeError, match="could not persist quality rule"):
        config.create_rule(_valid_rule(rule_id))

    assert redis_client.get(document_key) is None
    assert redis_client.get(index_key) == "corrupt-not-a-set"


def test_quality_mutations_require_live_catalog_and_deletion_intent_absence():
    redis_client, config = _config()
    namespace_intent = RK.meta_namespace_deletion_intent("org", "lake")
    table_intent = RK.meta_simple_deletion_intent("org", "lake", "facts")

    redis_client.set(namespace_intent, json.dumps({"intent_id": "delete"}))
    assert not config.set_schedule({"enabled": True})
    assert not config.set_table_config("facts", {"scope": "full"})
    assert not config.set_latest("facts", {"status": "ok"})
    with pytest.raises(RuntimeError, match="could not persist quality rule"):
        config.create_rule(_valid_rule("during-delete"))
    assert redis_client.get(config._key("schedule")) is None
    assert redis_client.get(config._key("latest", "facts")) is None

    redis_client.delete(namespace_intent)
    redis_client.set(table_intent, json.dumps({"intent_id": "delete-table"}))
    assert config.set_schedule({"enabled": True})
    assert not config.set_table_schedule("facts", {"enabled": True})
    assert not config.set_latest_column("facts", "amount", {"status": "ok"})
    assert not config.set_anomalies("facts", [])

    redis_client.delete(table_intent)
    redis_client.delete(RK.meta_leaf("org", "lake", "facts"))
    assert not config.set_latest("facts", {"status": "ok"})
    redis_client.delete(RK.meta_root("org", "lake"))
    assert not config.set_global_config({"checks": {}})


@pytest.mark.parametrize("field", ["version", "ts"])
def test_quality_mutations_reject_catalog_identities_unsafe_in_redis_lua(field):
    redis_client, config = _config()
    too_large = (1 << 53)

    root_key = RK.meta_root("org", "lake")
    root = json.loads(redis_client.get(root_key))
    root[field] = too_large
    redis_client.set(root_key, json.dumps(root))
    assert not config.set_global_config({"checks": {}})
    with pytest.raises(RuntimeError, match="could not persist quality rule"):
        config.create_rule(_valid_rule(f"unsafe-root-{field}"))

    _seed_live_catalog(redis_client)
    leaf_key = RK.meta_leaf("org", "lake", "facts")
    leaf = json.loads(redis_client.get(leaf_key))
    leaf[field] = too_large
    redis_client.set(leaf_key, json.dumps(leaf))
    assert not config.set_table_config("facts", {"scope": "full"})
    assert not config.set_latest("facts", {"status": "stale"})


@pytest.mark.parametrize(
    "corruption",
    [
        {"read_only": "garbage"},
        {"clone_type": "unknown"},
        {
            "read_only": False,
            "clone_type": "replica",
            "cloned_from": "source",
        },
        {"replica_tables": ["facts", "facts"]},
    ],
)
def test_quality_mutations_reject_corrupt_optional_root_lifecycle_fields(
    corruption,
):
    redis_client, config = _config()
    root_key = RK.meta_root("org", "lake")
    root = json.loads(redis_client.get(root_key))
    root.update(corruption)
    redis_client.set(root_key, json.dumps(root))

    assert not config.set_global_config({"checks": {}})
    assert not config.set_table_schedule("facts", {"enabled": True})
    assert not config.set_latest("facts", {"status": "stale"})
    with pytest.raises(RuntimeError, match="could not persist quality rule"):
        config.create_rule(_valid_rule("corrupt-root"))


def test_quality_table_mutation_is_fenced_against_delete_recreate_aba():
    redis_client, _ = _config()
    leaf_key = RK.meta_leaf("org", "lake", "facts")

    class RecreateBeforeMutation:
        def __init__(self, inner):
            self.inner = inner
            self.swapped = False

        def __getattr__(self, name):
            return getattr(self.inner, name)

        def eval(self, script, *args):
            if "quality-config lifecycle-fenced set" in script and not self.swapped:
                self.swapped = True
                self.inner.set(
                    leaf_key,
                    json.dumps({
                        "version": 0,
                        "ts": 2,
                        "path": "org/lake/facts/recreated-snapshot.json",
                    }),
                )
            return self.inner.eval(script, *args)

    raced = DQConfig(RecreateBeforeMutation(redis_client), "org", "lake")
    assert not raced.set_latest("facts", {"status": "stale"})
    assert redis_client.get(raced._key("latest", "facts")) is None


def test_bulk_quality_inventories_fail_closed_on_backend_or_document_error():
    redis_client, healthy = _config()

    class ScanFault:
        def __getattr__(self, name):
            return getattr(redis_client, name)

        def scan(self, *_args, **_kwargs):
            raise RuntimeError("injected SCAN failure")

    fault = DQConfig(ScanFault(), "org", "lake")
    with pytest.raises(DQConfigReadError, match="schedule inventory"):
        fault.get_all_table_schedules()
    with pytest.raises(DQConfigReadError, match="latest quality"):
        fault.get_all_latest()

    redis_client.set(healthy._key("schedule", "facts"), "[]")
    with pytest.raises(DQConfigReadError, match="wrong shape"):
        healthy.get_all_table_schedules()

    redis_client.delete(healthy._key("schedule", "facts"))
    redis_client.set(healthy._key("latest", "facts"), "not-json")
    with pytest.raises(DQConfigReadError, match="malformed"):
        healthy.get_all_latest()
