"""Opt-in real-Redis gate for the no-mirror snapshot publication script.

Set ``SUPERTABLE_TEST_REDIS_URL`` to a disposable standalone Redis database.
The test flushes the server-side script cache to model promotion/restart, so it
must never target a shared or production Redis instance.
"""
from __future__ import annotations

import json
import os
import uuid

import pytest
import redis

from supertable import redis_keys as RK
from supertable.redis_catalog import RedisCatalog


@pytest.mark.integration
def test_no_mirror_commit_survives_real_redis_noscript_failover():
    url = os.environ.get("SUPERTABLE_TEST_REDIS_URL", "").strip()
    if not url:
        pytest.skip("SUPERTABLE_TEST_REDIS_URL is not configured")

    client = redis.Redis.from_url(url, decode_responses=True)
    client.ping()
    identity = uuid.uuid4().hex
    organization = f"snapshot-e2e-{identity}"
    super_name = "lake"
    table_name = "records"
    token = f"owner-{identity}"
    json_ceiling = 99_999_999_999_999
    int64_max = (1 << 63) - 1
    catalog = RedisCatalog(redis_client=client)
    keys = [
        RK.meta_leaf(organization, super_name, table_name),
        RK.meta_root(organization, super_name),
        RK.lock_leaf(organization, super_name, table_name),
        RK.lock_namespace(organization, super_name),
        RK.meta_table_names(organization, super_name),
        RK.meta_namespace_deletion_intent(organization, super_name),
        RK.meta_simple_deletion_intent(
            organization, super_name, table_name,
        ),
        RK.schema(organization, super_name, table_name),
        catalog._quality_key(
            organization, super_name, "pending_unresolved", table_name,
        ),
        RK.meta_mirrors(organization, super_name),
    ]
    try:
        client.set(
            keys[0],
            json.dumps({
                "version": json_ceiling - 1,
                "ts": 1,
                "path": "snap/max-1.json",
                "payload": {"resources": [], "_row_filter": None},
            }),
        )
        client.set(
            keys[1],
            json.dumps({
                "version": json_ceiling - 1,
                "ts": 1,
                "read_only": False,
            }),
        )
        client.set(keys[2], token, ex=30)

        # A newly promoted Redis primary may retain data but not scripts.
        # Registered Script must issue EVALSHA, recover from NOSCRIPT by loading
        # this exact source, and retry the same fenced mutation.
        client.script_flush()
        assert catalog.commit_snapshot(
            organization,
            super_name,
            table_name,
            {
                "schema": [{"id": "long"}],
                "resources": [],
                "rowid_high_watermark": int64_max,
                "label": "snowman-☃",
            },
            'snap/quote"-max-☃.json',
            expected_version=json_ceiling - 1,
            expected_path="snap/max-1.json",
            lock_token=token,
            commit_id='commit"-5',
            expected_mirrors=[],
            expected_mirror_pin=None,
            quality_generation='commit"-5',
            now_ms=json_ceiling,
        ) == (json_ceiling, json_ceiling)

        leaf = json.loads(client.get(keys[0]))
        root = json.loads(client.get(keys[1]))
        assert leaf["path"] == 'snap/quote"-max-☃.json'
        assert leaf["commit_id"] == 'commit"-5'
        assert leaf["payload"]["resources"] == []
        assert leaf["payload"]["rowid_high_watermark"] == int64_max
        assert leaf["payload"]["label"] == "snowman-☃"
        assert type(leaf["version"]) is int
        assert type(leaf["ts"]) is int
        assert type(root["version"]) is int
        assert type(root["ts"]) is int
        assert leaf["version"] == json_ceiling
        assert leaf["ts"] == json_ceiling
        assert root["version"] == json_ceiling
        assert root["ts"] == json_ceiling
        assert root["commit_id"] == 'commit"-5'
        assert json.loads(client.get(keys[7])) == {"id": "long"}
        assert client.get(keys[8]) == 'commit"-5'

        before_leaf = client.get(keys[0])
        before_root = client.get(keys[1])
        with pytest.raises(ValueError, match="invalid Unicode surrogate"):
            catalog.commit_snapshot(
                organization,
                super_name,
                table_name,
                {"resources": [], "invalid": "\ud800"},
                "snap/rejected.json",
                expected_version=json_ceiling,
                expected_path='snap/quote"-max-☃.json',
                lock_token=token,
                commit_id="commit-rejected",
                expected_mirrors=[],
                expected_mirror_pin=None,
                now_ms=123,
            )
        with pytest.raises(ValueError, match="publication timestamp"):
            catalog.commit_snapshot(
                organization,
                super_name,
                table_name,
                {"resources": []},
                "snap/overflow.json",
                expected_version=json_ceiling,
                expected_path='snap/quote"-max-☃.json',
                lock_token=token,
                commit_id="commit-overflow",
                expected_mirrors=[],
                expected_mirror_pin=None,
                now_ms=json_ceiling + 1,
            )
        assert client.get(keys[0]) == before_leaf
        assert client.get(keys[1]) == before_root
    finally:
        client.delete(*keys)
