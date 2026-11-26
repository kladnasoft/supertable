"""
What a write does in Redis after its commit takes one round trip.

Once the table's new entry is committed, ``DataWriter.write`` stores the
table's schema document, adds the table's name to the lake's table names, and
mirroring then reads the lake's enabled mirror formats: a SET, a SADD and a
GET, each a round trip of its own.  ``store_schema_and_read_mirrors`` sends the
three together, in that order.

The three calls as they were are kept below.  Both ways run against a fake
Redis in the same state, and what is stored, what is returned and what is
raised must be the same.  ``get_mirrors`` had its parsing moved into a function
the new method shares; it is compared with its old body too.
"""
from __future__ import annotations

import json
import logging
import os
from types import SimpleNamespace

import fakeredis
import pytest
import redis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import redis_catalog  # noqa: E402
from supertable import redis_keys as RK  # noqa: E402
from supertable.redis_catalog import RedisCatalog  # noqa: E402

ORG, LAKE, TABLE = "acme", "sales", "orders"
SCHEMA = json.dumps({"id": "Int64", "név": "String"}, ensure_ascii=False)
# The library logs through this one logger; naming it makes the capture hold
# whatever an earlier test left its level at.
LOGGER = redis_catalog.logger.name
SCHEMA_KEY, NAMES_KEY, MIRRORS_KEY = (
    RK.schema(ORG, LAKE, TABLE), RK.meta_table_names(ORG, LAKE), RK.meta_mirrors(ORG, LAKE))


# ---------------------------------------------------------------------------
# As it was at 117bcf0
# ---------------------------------------------------------------------------

def _get_mirrors_as_it_was(catalog, org, sup):
    try:
        raw = catalog.r.get(RK.meta_mirrors(org, sup))
        if not raw:
            return []
        obj = json.loads(raw)
        formats = obj.get("formats", [])
        seen = set()
        out = []
        for f in (formats or []):
            fu = str(f).upper()
            if fu in ("DELTA", "ICEBERG", "PARQUET") and fu not in seen:
                seen.add(fu)
                out.append(fu)
        return out
    except redis.RedisError as e:
        redis_catalog.logger.error(f"[redis-catalog] get_mirrors error: {e}")
        return []
    except Exception:
        return []


def _three_calls_as_they_were(catalog, org, sup, simple, schema_json):
    """The two lines of ``DataWriter.write`` and the read ``mirror_if_enabled``
    began with."""
    catalog.r.set(RK.schema(org, sup, simple), schema_json)
    catalog.r.sadd(RK.meta_table_names(org, sup), simple)
    return _get_mirrors_as_it_was(catalog, org, sup)


# ---------------------------------------------------------------------------
# A catalog on a fake Redis that records its round trips
# ---------------------------------------------------------------------------

class _Side:

    def __init__(self, monkeypatch):
        self.client = fakeredis.FakeRedis(server=fakeredis.FakeServer(), decode_responses=True)
        self.trips = []
        real_command, real_pipeline = self.client.execute_command, self.client.pipeline

        def command(*args, **options):
            self.trips.append([str(args[0]).upper()])
            return real_command(*args, **options)

        def pipeline(*args, **options):
            pipe = real_pipeline(*args, **options)
            real_execute = pipe.execute

            def execute(*a, **k):
                self.trips.append(["MULTI"] * bool(pipe.transaction)
                                  + [str(c[0][0]).upper() for c in pipe.command_stack])
                return real_execute(*a, **k)

            pipe.execute = execute
            return pipe

        self.client.execute_command, self.client.pipeline = command, pipeline
        with monkeypatch.context() as patched:
            patched.setattr(
                redis_catalog, "RedisConnector", lambda options=None: SimpleNamespace(r=self.client))
            self.catalog = RedisCatalog()

    def put(self, mirrors=None, schema=None, names=()):
        if mirrors is not None:
            self.client.set(MIRRORS_KEY, mirrors)
        if schema is not None:
            self.client.set(SCHEMA_KEY, schema)
        if names:
            self.client.sadd(NAMES_KEY, *names)
        self.trips.clear()

    def state(self):
        out = {}
        for key in sorted(self.client.keys("*")):
            kind = self.client.type(key)
            out[key] = sorted(self.client.smembers(key)) if kind == "set" else (
                self.client.hgetall(key) if kind == "hash" else self.client.get(key))
        return out


@pytest.fixture
def side(monkeypatch):
    return _Side(monkeypatch)


@pytest.fixture
def twin(monkeypatch):
    return _Side(monkeypatch), _Side(monkeypatch)


def _run(function, *args):
    try:
        return ("returned", function(*args))
    except Exception as error:   # noqa: BLE001 - whatever it raises is the outcome
        return ("raised", type(error).__name__, str(error))


MIRRORS = {
    "no mirrors key": None,
    "one format": json.dumps({"formats": ["DELTA"], "ts": 1}),
    "three formats": json.dumps({"formats": ["PARQUET", "DELTA", "ICEBERG"], "ts": 1}),
    "lower case, repeated and unknown": json.dumps({"formats": ["delta", "DELTA", "csv", "Iceberg"]}),
    "no formats": json.dumps({"formats": [], "ts": 1}),
    "formats that are null": json.dumps({"formats": None}),
    "formats that are one text": json.dumps({"formats": "DELTA"}),
    "formats that are numbers": json.dumps({"formats": [1, 2]}),
    "an object without formats": "{}",
    "a list": '["DELTA"]',
    "a number": "7",
    "JSON null": "null",
    "text that is not JSON": "{not json",
    "an empty value": "",
}


# ---------------------------------------------------------------------------
# The same stored, the same returned
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", sorted(MIRRORS))
@pytest.mark.parametrize("before", ["a new table", "a table written before"])
def test_it_stores_and_returns_what_the_three_calls_did(twin, name, before):
    was_side, now_side = twin
    for one in twin:
        if before == "a table written before":
            one.put(MIRRORS[name], schema='{"id": "Int32"}', names=("orders", "customers"))
        else:
            one.put(MIRRORS[name])

    was = _run(_three_calls_as_they_were, was_side.catalog, ORG, LAKE, TABLE, SCHEMA)
    now = _run(now_side.catalog.store_schema_and_read_mirrors, ORG, LAKE, TABLE, SCHEMA)

    assert now == was
    assert now[0] == "returned" and isinstance(now[1], list)
    assert now_side.state() == was_side.state()
    assert now_side.client.get(SCHEMA_KEY) == SCHEMA
    assert TABLE in now_side.client.smembers(NAMES_KEY)


def test_the_formats_come_back_in_the_order_they_were_enabled(side):
    side.put(MIRRORS["three formats"])

    assert side.catalog.store_schema_and_read_mirrors(ORG, LAKE, TABLE, SCHEMA) == [
        "PARQUET", "DELTA", "ICEBERG"]


@pytest.mark.parametrize("name", sorted(MIRRORS))
def test_get_mirrors_reads_what_it_read(twin, name):
    was_side, now_side = twin
    for one in twin:
        one.put(MIRRORS[name])

    assert _run(now_side.catalog.get_mirrors, ORG, LAKE) == _run(
        _get_mirrors_as_it_was, was_side.catalog, ORG, LAKE)
    assert now_side.trips == was_side.trips == [["GET"]]


def test_get_mirrors_still_logs_a_read_redis_refuses(twin, caplog):
    was_side, now_side = twin
    for one in twin:
        one.client.hset(MIRRORS_KEY, "formats", "DELTA")
        one.put()

    with caplog.at_level(logging.ERROR, logger=LOGGER):
        was = _run(_get_mirrors_as_it_was, was_side.catalog, ORG, LAKE)
        now = _run(now_side.catalog.get_mirrors, ORG, LAKE)

    assert now == was == ("returned", [])
    logged = [r.getMessage() for r in caplog.records if "get_mirrors error" in r.getMessage()]
    assert len(logged) == 2 and logged[0] == logged[1]


# ---------------------------------------------------------------------------
# One round trip
# ---------------------------------------------------------------------------

def test_three_round_trips_became_one(twin):
    was_side, now_side = twin
    for one in twin:
        one.put(MIRRORS["one format"])

    _three_calls_as_they_were(was_side.catalog, ORG, LAKE, TABLE, SCHEMA)
    now_side.catalog.store_schema_and_read_mirrors(ORG, LAKE, TABLE, SCHEMA)

    assert was_side.trips == [["SET"], ["SADD"], ["GET"]]
    assert now_side.trips == [["SET", "SADD", "GET"]], "the same commands, in order, not a transaction"


# ---------------------------------------------------------------------------
# What Redis refuses
# ---------------------------------------------------------------------------

def test_a_mirrors_key_of_the_wrong_kind_reads_as_no_formats_and_is_logged(twin, caplog):
    was_side, now_side = twin
    for one in twin:
        one.client.hset(MIRRORS_KEY, "formats", "DELTA")
        one.put()

    with caplog.at_level(logging.ERROR, logger=LOGGER):
        was = _run(_three_calls_as_they_were, was_side.catalog, ORG, LAKE, TABLE, SCHEMA)
        now = _run(now_side.catalog.store_schema_and_read_mirrors, ORG, LAKE, TABLE, SCHEMA)

    assert now == was == ("returned", [])
    logged = [r.getMessage() for r in caplog.records if "get_mirrors error" in r.getMessage()]
    assert len(logged) == 2 and logged[0] == logged[1], "the same line, once from each"
    assert now_side.state() == was_side.state()


def test_names_of_the_wrong_kind_raise_what_the_sadd_raised(twin):
    was_side, now_side = twin
    for one in twin:
        one.client.set(NAMES_KEY, "not a set")
        one.put(MIRRORS["one format"])

    was = _run(_three_calls_as_they_were, was_side.catalog, ORG, LAKE, TABLE, SCHEMA)
    now = _run(now_side.catalog.store_schema_and_read_mirrors, ORG, LAKE, TABLE, SCHEMA)

    assert now == was
    assert now[:2] == ("raised", "ResponseError")
    assert now_side.state() == was_side.state(), "the schema was stored on both"


class _Refusing:
    """A pipeline that answers one of its commands with an error."""

    def __init__(self, pipe, position, error):
        self._pipe, self._position, self._error = pipe, position, error

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._pipe.reset()

    def __getattr__(self, name):
        return getattr(self._pipe, name)

    def execute(self, raise_on_error=True):
        results = self._pipe.execute(raise_on_error=False)
        results[self._position] = self._error
        if raise_on_error:
            raise self._error
        return results


def _refuse(side: _Side, position: int, error: Exception) -> None:
    real = side.client.pipeline
    side.client.pipeline = lambda *a, **k: _Refusing(real(*a, **k), position, error)


@pytest.mark.parametrize("position, command", [(0, "the SET"), (1, "the SADD")])
def test_a_refused_write_raises_after_all_three_were_sent(side, position, command):
    """The caller answers it as it answered a failing SET or SADD: it logs the
    failure and lets mirroring read the formats itself."""
    side.put(MIRRORS["one format"])
    refusal = redis.ResponseError("OOM command not allowed when used memory > 'maxmemory'.")
    _refuse(side, position, refusal)

    with pytest.raises(redis.ResponseError) as raised:
        side.catalog.store_schema_and_read_mirrors(ORG, LAKE, TABLE, SCHEMA)

    assert raised.value is refusal
    assert side.trips == [["SET", "SADD", "GET"]]


def test_the_set_is_named_first_when_both_writes_are_refused(side):
    side.put()
    first, second = redis.ResponseError("the SET"), redis.ResponseError("the SADD")
    real = side.client.pipeline

    class _Both(_Refusing):
        def execute(self, raise_on_error=True):
            results = self._pipe.execute(raise_on_error=False)
            results[0], results[1] = first, second
            return results

    side.client.pipeline = lambda *a, **k: _Both(real(*a, **k), 0, first)

    with pytest.raises(redis.ResponseError) as raised:
        side.catalog.store_schema_and_read_mirrors(ORG, LAKE, TABLE, SCHEMA)

    assert raised.value is first


def test_a_refused_read_is_no_formats_as_get_mirrors_answers_it(side, caplog):
    side.put(MIRRORS["one format"])
    _refuse(side, 2, redis.ResponseError("NOPERM this user has no permissions to run the 'get' command"))

    with caplog.at_level(logging.ERROR, logger=LOGGER):
        assert side.catalog.store_schema_and_read_mirrors(ORG, LAKE, TABLE, SCHEMA) == []

    assert any("[redis-catalog] get_mirrors error: NOPERM" in r.getMessage() for r in caplog.records)
    assert side.client.get(SCHEMA_KEY) == SCHEMA and TABLE in side.client.smembers(NAMES_KEY)


def test_a_read_that_fails_some_other_way_is_no_formats_and_not_logged(side, caplog):
    side.put(MIRRORS["one format"])
    _refuse(side, 2, ValueError("not an answer"))

    with caplog.at_level(logging.ERROR, logger=LOGGER):
        assert side.catalog.store_schema_and_read_mirrors(ORG, LAKE, TABLE, SCHEMA) == []

    assert not [r for r in caplog.records if "get_mirrors" in r.getMessage()]


def test_a_connection_that_fails_raises(side):
    side.put(MIRRORS["one format"])
    real = side.client.pipeline

    class _Down(_Refusing):
        def execute(self, raise_on_error=True):
            raise redis.exceptions.ConnectionError("Connection refused")

    side.client.pipeline = lambda *a, **k: _Down(real(*a, **k), 0, None)

    with pytest.raises(redis.exceptions.ConnectionError):
        side.catalog.store_schema_and_read_mirrors(ORG, LAKE, TABLE, SCHEMA)

    assert side.state() == {MIRRORS_KEY: MIRRORS["one format"]}, "nothing was stored"
