"""The two provider-share primitives dataisland-core revokes and serves with.

``delete_share_if_unchanged`` is how Core revokes a share or a publication: it
re-reads the document, re-authorizes against it, and must delete exactly that
document, never one a concurrent writer put in its place.
``allocate_share_manifest_generation`` orders every manifest Core issues for
one share token by Redis time, strictly, across processes.

These run the catalog's real Lua on fakeredis, through ``RedisCatalog()``'s
own constructor, so the scripts are registered the way production registers
them.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import fakeredis
import pytest
import redis

from supertable import redis_catalog
from supertable import redis_keys as RK

ORG = "acme"
SHARE = "share-1"
TOKEN = "ab" * 32


@pytest.fixture
def client():
    return fakeredis.FakeRedis(server=fakeredis.FakeServer(), decode_responses=True)


@pytest.fixture
def catalog(client, monkeypatch):
    monkeypatch.setattr(
        redis_catalog, "RedisConnector", lambda options=None: SimpleNamespace(r=client))
    return redis_catalog.RedisCatalog()


def _doc(**extra):
    return {"share_id": SHARE, "label": "Ügyfél 🚀", "tables": ["orders"],
            "enabled": True, "limit": 2 ** 53 - 1, **extra}


class TestDeleteShareIfUnchanged:

    def test_an_unchanged_share_goes_with_its_index_entry(self, catalog, client):
        catalog.create_share(ORG, SHARE, _doc())
        stored = catalog.get_share(ORG, SHARE)

        assert catalog.delete_share_if_unchanged(ORG, SHARE, stored) is True
        assert client.exists(RK.share_doc(ORG, SHARE)) == 0
        assert not client.sismember(RK.share_index(ORG), SHARE)
        assert catalog.list_shares(ORG) == []

    def test_a_replaced_share_survives_and_the_call_raises(self, catalog, client):
        catalog.create_share(ORG, SHARE, _doc())
        decided_on = catalog.get_share(ORG, SHARE)
        catalog.create_share(ORG, SHARE, _doc(enabled=False))  # replaced meanwhile

        with pytest.raises(RuntimeError, match="Share changed during deletion"):
            catalog.delete_share_if_unchanged(ORG, SHARE, decided_on)
        assert catalog.get_share(ORG, SHARE)["enabled"] is False
        assert client.sismember(RK.share_index(ORG), SHARE)

    def test_the_same_content_in_another_key_order_is_not_the_stored_document(
            self, catalog):
        catalog.create_share(ORG, SHARE, _doc())
        reordered = dict(reversed(list(catalog.get_share(ORG, SHARE).items())))

        with pytest.raises(RuntimeError, match="Share changed during deletion"):
            catalog.delete_share_if_unchanged(ORG, SHARE, reordered)
        assert catalog.get_share(ORG, SHARE) == _doc()

    def test_an_absent_share_is_false(self, catalog):
        assert catalog.delete_share_if_unchanged(ORG, SHARE, _doc()) is False

    def test_an_index_entry_without_a_document_is_corruption(self, catalog, client):
        client.sadd(RK.share_index(ORG), SHARE)
        with pytest.raises(RuntimeError, match="Corrupt share metadata/index"):
            catalog.delete_share_if_unchanged(ORG, SHARE, _doc())
        assert client.sismember(RK.share_index(ORG), SHARE)

    def test_a_document_missing_from_the_index_is_corruption(self, catalog, client):
        client.set(RK.share_doc(ORG, SHARE), json.dumps(_doc()))
        with pytest.raises(RuntimeError, match="Corrupt share metadata/index"):
            catalog.delete_share_if_unchanged(ORG, SHARE, _doc())
        assert client.exists(RK.share_doc(ORG, SHARE)) == 1

    @pytest.mark.parametrize("wrong", ["doc", "index"])
    def test_a_key_of_the_wrong_type_is_corruption(self, catalog, client, wrong):
        if wrong == "doc":
            client.hset(RK.share_doc(ORG, SHARE), "a", "b")
            client.sadd(RK.share_index(ORG), SHARE)
        else:
            client.set(RK.share_doc(ORG, SHARE), json.dumps(_doc()))
            client.set(RK.share_index(ORG), "not-a-set")
        with pytest.raises(RuntimeError, match="Corrupt share metadata/index"):
            catalog.delete_share_if_unchanged(ORG, SHARE, _doc())

    def test_the_expected_document_must_be_a_dict(self, catalog):
        with pytest.raises(TypeError):
            catalog.delete_share_if_unchanged(ORG, SHARE, json.dumps(_doc()))

    def test_a_redis_error_propagates(self, catalog):
        catalog._share_delete_if_unchanged = _raising(redis.exceptions.TimeoutError)
        with pytest.raises(redis.exceptions.TimeoutError):
            catalog.delete_share_if_unchanged(ORG, SHARE, _doc())


class TestAllocateShareManifestGeneration:

    def _key(self, token=TOKEN):
        return RK.share_manifest_generation(ORG, SHARE, token)

    def test_generations_are_redis_milliseconds_and_strictly_increase(
            self, catalog, client):
        seconds, micros = client.time()
        before_ms = seconds * 1000 + micros // 1000
        values = [catalog.allocate_share_manifest_generation(ORG, SHARE, TOKEN)
                  for _ in range(200)]

        assert values[0] >= before_ms
        assert all(b > a for a, b in zip(values, values[1:]))
        assert int(client.get(self._key())) == values[-1]

    def test_a_stored_value_ahead_of_redis_time_is_still_exceeded(
            self, catalog, client):
        ahead = 4_000_000_000_000  # the year 2096
        client.set(self._key(), str(ahead))
        assert catalog.allocate_share_manifest_generation(ORG, SHARE, TOKEN) == ahead + 1
        assert catalog.allocate_share_manifest_generation(ORG, SHARE, TOKEN) == ahead + 2

    def test_each_token_counts_on_its_own(self, catalog, client):
        ahead = 4_000_000_000_000
        client.set(self._key(), str(ahead))
        assert catalog.allocate_share_manifest_generation(ORG, SHARE, "cd" * 32) < ahead

    @pytest.mark.parametrize("token", [
        "AB" * 32, "ab" * 31 + "a", "ab" * 32 + "a", "zz" * 32, "", None, 7])
    def test_a_malformed_incarnation_is_refused(self, catalog, client, token):
        with pytest.raises(ValueError):
            catalog.allocate_share_manifest_generation(ORG, SHARE, token)
        assert client.keys("*") == []

    @pytest.mark.parametrize("stored", ["abc", "-5", "1.5"])
    def test_a_stored_value_that_is_not_a_generation_is_corruption(
            self, catalog, client, stored):
        client.set(self._key(), stored)
        with pytest.raises(RuntimeError, match="Corrupt share manifest generation"):
            catalog.allocate_share_manifest_generation(ORG, SHARE, TOKEN)
        assert client.get(self._key()) == stored

    def test_the_sequence_stops_at_the_largest_exact_lua_integer(self, catalog, client):
        client.set(self._key(), str(2 ** 53 - 1))
        with pytest.raises(OverflowError):
            catalog.allocate_share_manifest_generation(ORG, SHARE, TOKEN)
        assert client.get(self._key()) == str(2 ** 53 - 1)

    def test_a_key_of_the_wrong_type_propagates(self, catalog, client):
        client.hset(self._key(), "a", "b")
        with pytest.raises(redis.exceptions.ResponseError):
            catalog.allocate_share_manifest_generation(ORG, SHARE, TOKEN)

    def test_a_redis_error_propagates(self, catalog):
        catalog._share_manifest_generation = _raising(redis.exceptions.TimeoutError)
        with pytest.raises(redis.exceptions.TimeoutError):
            catalog.allocate_share_manifest_generation(ORG, SHARE, TOKEN)

    def test_the_generation_key_is_not_a_listed_share(self, catalog):
        catalog.create_share(ORG, SHARE, _doc())
        catalog.allocate_share_manifest_generation(ORG, SHARE, TOKEN)
        assert catalog.list_shares(ORG) == [_doc()]


def _raising(error):
    def script(*args, **kwargs):
        raise error("Timeout reading from socket")
    return script
