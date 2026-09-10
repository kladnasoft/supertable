# route: supertable.tests.test_defect_fixes
"""Regression seals for defects found by auditing the write path.

Each test names the exact failure it prevents, so a future refactor that
reintroduces the behaviour fails here with the reason attached.
"""
from __future__ import annotations

import json

import fakeredis
import pytest

from supertable import redis_keys as RK


# ===========================================================================
# 1. RK.staging / RK.pipe did not exist -> AttributeError on every call
# ===========================================================================

class TestStagingPipeKeyBuilders:
    """redis_catalog called RK.staging()/RK.pipe(); redis_keys only ever
    defined staging_doc()/pipe_doc().  Every staging- and pipe-metadata
    method raised AttributeError, which the surrounding
    `except redis.RedisError` did not catch."""

    def test_staging_and_pipe_key_helpers_are_resolvable(self):
        import supertable.redis_catalog as rc
        import inspect

        src = inspect.getsource(rc)
        for bad in ("RK.staging(", "RK.pipe("):
            assert bad not in src, (
                f"{bad}) is not defined in redis_keys — use staging_doc/pipe_doc"
            )

    def test_staging_meta_roundtrip(self):
        from supertable.redis_catalog import RedisCatalog

        cat = RedisCatalog.__new__(RedisCatalog)
        cat.r = fakeredis.FakeRedis(decode_responses=True)

        assert cat.upsert_staging_meta("org", "sup", "stg", {"path": "/x"}) is True
        got = cat.get_staging_meta("org", "sup", "stg")
        assert got is not None, "staging meta must round-trip, not raise"
        assert got["path"] == "/x"
        assert got["staging_name"] == "stg"

    def test_pipe_meta_roundtrip(self):
        from supertable.redis_catalog import RedisCatalog

        cat = RedisCatalog.__new__(RedisCatalog)
        cat.r = fakeredis.FakeRedis(decode_responses=True)

        assert cat.upsert_pipe_meta(
            "org", "sup", "stg", "p1", {"simple_name": "t"}
        ) is True
        got = cat.get_pipe_meta("org", "sup", "stg", "p1")
        assert got is not None, "pipe meta must round-trip, not raise"
        assert got["simple_name"] == "t"


# ===========================================================================
# 2. bump_root replaced the whole root document
# ===========================================================================

class TestBumpRootPreservesFlags:
    """bump_root re-encoded only {version, ts} and SET it, destroying the
    clone/replica/read_only flags update_root_flags stores in the same key.
    _resolve_replica_info and the RBAC read-only guard read those back, so a
    single write silently disabled both."""

    @staticmethod
    def _catalog():
        from supertable.redis_catalog import RedisCatalog

        cat = RedisCatalog.__new__(RedisCatalog)
        cat.r = fakeredis.FakeRedis(decode_responses=True)
        cat._root_bump = cat.r.register_script(RedisCatalog._LUA_ROOT_BUMP)
        return cat

    def test_flags_survive_a_bump(self):
        cat = self._catalog()
        cat.ensure_root("org", "sup")
        cat.update_root_flags("org", "sup", {
            "read_only": True,
            "clone_type": "replica",
            "cloned_from": "source_sup",
            "replica_tables": ["a", "b"],
        })

        new_version = cat.bump_root("org", "sup", now_ms=1_700_000_000_000)

        root = cat.get_root("org", "sup")
        assert root["read_only"] is True, "read_only must survive a write"
        assert root["clone_type"] == "replica"
        assert root["cloned_from"] == "source_sup"
        assert root["replica_tables"] == ["a", "b"]
        assert root["version"] == new_version
        assert root["ts"] == 1_700_000_000_000

    def test_version_still_increments_monotonically(self):
        cat = self._catalog()
        cat.ensure_root("org", "sup")
        versions = [cat.bump_root("org", "sup", now_ms=1000 + i) for i in range(4)]
        assert versions == sorted(versions)
        assert len(set(versions)) == len(versions)

    def test_bump_on_missing_root_starts_at_zero(self):
        cat = self._catalog()
        assert cat.bump_root("org", "sup", now_ms=1) == 0

    def test_corrupt_root_is_replaced_not_crashed(self):
        cat = self._catalog()
        cat.r.set(RK.meta_root("org", "sup"), "not json at all")
        assert cat.bump_root("org", "sup", now_ms=1) == 0


# ===========================================================================
# 3. role_name=None raised AttributeError instead of PermissionError
# ===========================================================================

class TestRoleLookupRejectsBadNames:
    """rbac_get_role_id_by_name did role_name.lower() unguarded, so a None
    role escaped check_write_access as AttributeError — invisible to callers
    catching PermissionError."""

    @staticmethod
    def _catalog():
        from supertable.redis_catalog import RedisCatalog

        cat = RedisCatalog.__new__(RedisCatalog)
        cat.r = fakeredis.FakeRedis(decode_responses=True)
        return cat

    @pytest.mark.parametrize("bad", [None, "", 123, object()])
    def test_bad_role_name_resolves_to_none(self, bad):
        cat = self._catalog()
        assert cat.rbac_get_role_id_by_name("org", "sup", bad) is None

    def test_valid_name_still_resolves(self):
        cat = self._catalog()
        cat.r.hset(RK.rbac_rolename_to_id("org", "sup"), "admin", "role-1")
        assert cat.rbac_get_role_id_by_name("org", "sup", "AdMiN") == "role-1"


# ===========================================================================
# 4. Audit chain-head key rejected ordinary hostnames
# ===========================================================================

class TestAuditChainHeadKeySafety:
    """INSTANCE_ID is f"{hostname}-{pid}" but key segments must match
    ^[a-z0-9][a-z0-9_-]{0,63}$.  An uppercase letter or a dot (any FQDN)
    made the key builder raise; both callers swallowed it, so the chain head
    never persisted and every restart silently began from genesis."""

    @staticmethod
    def _writer(instance_id):
        from supertable.audit.writer_redis import RedisAuditWriter

        return RedisAuditWriter(
            redis_client=fakeredis.FakeRedis(decode_responses=True),
            org="org",
            instance_id=instance_id,
        )

    @pytest.mark.parametrize("instance_id", [
        "Host.Example.COM-1234",
        "web-01.prod.internal-9",
        "UPPER-77",
        "..--weird..-1",
    ])
    def test_chain_head_key_is_buildable_for_real_hostnames(self, instance_id):
        w = self._writer(instance_id)
        key = w._chain_key          # must not raise
        assert key.startswith("supertable:org:system:audit:chain_head:doc:")

    def test_chain_head_actually_persists_and_reloads(self):
        w = self._writer("Host.Example.COM-1234")
        w.save_chain_head("a" * 64, 7)
        head, count = w.load_chain_head()
        assert head == "a" * 64, "chain head must survive a restart"
        assert count == 7

    def test_already_valid_ids_are_untouched(self):
        from supertable.audit.writer_redis import RedisAuditWriter

        assert RedisAuditWriter._key_safe_instance_id("web01-42") == "web01-42"

    def test_distinct_hosts_stay_distinct(self):
        from supertable.audit.writer_redis import RedisAuditWriter

        a = RedisAuditWriter._key_safe_instance_id("host-a.example.com-1")
        b = RedisAuditWriter._key_safe_instance_id("host-b.example.com-1")
        assert a != b


# ===========================================================================
# 5. The audit hash chain did not cover event content
# ===========================================================================

class TestChainCoversContent:
    """advance() was called with event ids only, so editing an event's
    detail/actor/outcome in place left the chain verifying cleanly as long as
    event_id was preserved."""

    def test_content_change_changes_the_batch_hash(self):
        from supertable.audit.chain import compute_batch_hash, compute_content_hash

        ids = ["e1", "e2"]
        h_before = compute_batch_hash(ids, compute_content_hash(["c1", "c2"]))
        h_after = compute_batch_hash(ids, compute_content_hash(["c1", "TAMPERED"]))
        assert h_before != h_after, "same ids + different content must differ"

    def test_content_hash_is_order_independent(self):
        from supertable.audit.chain import compute_content_hash

        assert compute_content_hash(["b", "a"]) == compute_content_hash(["a", "b"])

    def test_event_field_edit_is_detectable(self):
        from supertable.audit.chain import compute_content_hash
        from supertable.audit.events import AuditEvent

        def _ev(detail):
            return AuditEvent(
                event_id="fixed-id", timestamp_ms=1, category="data_mutation",
                action="data_write", organization="org", detail=detail,
            )

        honest = compute_content_hash([_ev('{"rows":1}').event_hash()])
        tampered = compute_content_hash([_ev('{"rows":999}').event_hash()])
        assert honest != tampered, (
            "editing detail while keeping event_id must break the chain"
        )
