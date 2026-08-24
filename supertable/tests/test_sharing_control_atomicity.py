"""Distributed atomicity contracts for provider and linked-share controls."""
from __future__ import annotations

import json
import threading

import fakeredis

from supertable import redis_keys as RK
from supertable.redis_catalog import RedisCatalog


def _catalogs() -> tuple[RedisCatalog, RedisCatalog, fakeredis.FakeStrictRedis]:
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    return (
        RedisCatalog(redis_client=client),
        RedisCatalog(redis_client=client),
        client,
    )


def _run_race(*operations):
    barrier = threading.Barrier(len(operations))
    results = []
    errors = []

    def invoke(operation):
        barrier.wait(timeout=5)
        try:
            results.append(operation())
        except BaseException as exc:  # asserted by each test
            errors.append(exc)

    workers = [threading.Thread(target=invoke, args=(operation,)) for operation in operations]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=10)
        assert not worker.is_alive()
    return results, errors


def _share_document(share_id: str, *, label: str = "share") -> dict:
    return {
        "share_id": share_id,
        "organization": "acme",
        "super_name": "lake",
        "tables": ["orders"],
        "grantee_org": "partner",
        "token_hash": "a" * 64,
        "created_ms": 1,
        "enabled": True,
        "label": label,
    }


def _linked_document(link_id: str) -> dict:
    return {
        "link_id": link_id,
        "provider_url": "https://provider.invalid/manifest",
        "provider_token": "secret",
        "cached_manifest": {"tables": []},
        "expires_ms": 1,
    }


def test_provider_share_quota_is_atomic_across_catalog_instances():
    first, second, client = _catalogs()
    results, errors = _run_race(
        lambda: first.create_share(
            "acme", "share-a", _share_document("share-a"), max_items=1,
        ),
        lambda: second.create_share(
            "acme", "share-b", _share_document("share-b"), max_items=1,
        ),
    )

    assert results == [None]
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert "safety limit" in str(errors[0])
    assert client.scard(RK.share_index("acme")) == 1


def test_linked_share_quota_is_atomic_across_catalog_instances():
    first, second, client = _catalogs()
    client.set(
        RK.meta_root("acme", "lake"),
        json.dumps({"version": 0, "ts": 1}),
    )
    results, errors = _run_race(
        lambda: first.create_linked_share(
            "acme", "lake", "link-a", _linked_document("link-a"),
            max_items=1,
        ),
        lambda: second.create_linked_share(
            "acme", "lake", "link-b", _linked_document("link-b"),
            max_items=1,
        ),
    )

    assert results == [None]
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert "safety limit" in str(errors[0])
    assert client.scard(RK.linked_share_index("acme", "lake")) == 1


def test_conditional_revoke_never_deletes_a_concurrent_replacement():
    first, second, _client = _catalogs()
    original = _share_document("share-a", label="original")
    replacement = _share_document("share-a", label="replacement")
    first.create_share("acme", "share-a", original, max_items=10)
    expected = first.get_share("acme", "share-a")

    results, errors = _run_race(
        lambda: first.delete_share_if_unchanged(
            "acme", "share-a", expected,
        ),
        lambda: second.update_share("acme", "share-a", replacement),
    )

    current = first.get_share("acme", "share-a")
    if current is None:
        assert sorted(results) == [False, True]
        assert errors == []
    else:
        assert current == replacement
        assert results == [True]
        assert len(errors) == 1
        assert isinstance(errors[0], RuntimeError)
        assert "changed during deletion" in str(errors[0])
