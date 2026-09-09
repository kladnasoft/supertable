"""Bounded catalog discovery keeps scan pages separate from payload batches."""

import json
from unittest.mock import Mock

import fakeredis
import pytest

from supertable import redis_keys as RK
from supertable.redis_catalog import RedisCatalog


def test_catalog_leaf_scan_separates_scan_pages_from_payload_batches(monkeypatch):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    catalog.ensure_root("org", "lake")
    snapshot = {
        "snapshot_version": 4,
        "schema": [],
        "resources": [],
        "tombstone": None,
        "tombstone_rows": 0,
        "tombstone_digest": None,
    }
    client.set(RK.meta_leaf("org", "lake", "facts"), json.dumps({
        "version": 4,
        "ts": 1,
        "path": "org/lake/tables/facts/snapshots/current.json",
        "payload": snapshot,
    }))
    client.set(RK.meta_leaf("org", "lake", "other"), json.dumps({
        "version": 2,
        "ts": 1,
        "path": "org/lake/tables/other/snapshots/current.json",
        "payload": snapshot,
    }))
    scan_options = []
    fetch_batches = []
    original_scan = catalog._scan_leaf_keys_raw
    original_fetch = catalog._fetch_batch

    def tracked_scan(*args, **kwargs):
        scan_options.append(kwargs)
        yield from original_scan(*args, **kwargs)

    def tracked_fetch(keys):
        fetch_batches.append(tuple(keys))
        yield from original_fetch(keys)

    monkeypatch.setattr(catalog, "_scan_leaf_keys_raw", tracked_scan)
    monkeypatch.setattr(catalog, "_fetch_batch", tracked_fetch)

    items = list(catalog.scan_leaf_items(
        "org",
        "lake",
        count=512,
        batch_size=1,
        max_scan_calls=7,
    ))

    assert {item["simple"] for item in items} == {"facts", "other"}
    assert scan_options == [{
        "allowed": None,
        "count": 512,
        "max_scan_calls": 7,
    }]
    assert len(fetch_batches) == 2
    assert all(len(batch) == 1 for batch in fetch_batches)


def test_catalog_leaf_scan_fails_closed_at_scan_call_bound(monkeypatch):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    scan = Mock(return_value=(1, []))
    monkeypatch.setattr(client, "scan", scan)

    with pytest.raises(RuntimeError, match="SCAN exceeded its call safety bound"):
        list(catalog._scan_leaf_keys_raw(
            "org",
            "lake",
            allowed=None,
            count=512,
            max_scan_calls=2,
        ))

    assert scan.call_count == 2
