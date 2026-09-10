"""``DEFAULT_LOCK_DURATION_SEC`` must actually drive every lock's TTL.

The setting existed, was parsed from the environment and propagated between
``Settings`` and ``Default`` -- but every lock call site passed a literal
``ttl_s=30``, so changing it did nothing at all.  It looked like dead config
and was very nearly deleted; it is in fact an unwired feature.

These tests pin the wiring end to end, so a future refactor that reintroduces
a hardcoded TTL fails here instead of silently ignoring the operator's value.

Semantics being sealed: the TTL is the CRASH-RECOVERY window.  The holder's
heartbeat renews the lease at half the TTL for as long as the operation runs,
so a long write or compaction never expires its own lock; the TTL only bounds
how long a *dead* holder's lock lingers before another writer may take it.
"""
from __future__ import annotations

import fakeredis
import pytest

from supertable import redis_keys as RK
from supertable.redis_catalog import RedisCatalog


@pytest.fixture
def catalog(monkeypatch):
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    return RedisCatalog(redis_client=client), client


def _set_ttl(monkeypatch, seconds: int) -> None:
    """Swap in a settings object carrying `seconds`.

    ``Settings`` is a frozen dataclass, so the value cannot be mutated in
    place; replace the object the catalog resolves against instead.
    """
    import dataclasses

    import supertable.redis_catalog as rc
    from supertable.config.settings import settings as real_settings

    monkeypatch.setattr(
        rc,
        "settings",
        dataclasses.replace(real_settings, DEFAULT_LOCK_DURATION_SEC=seconds),
        raising=True,
    )


@pytest.mark.parametrize("configured", [7, 30, 120])
def test_table_lock_ttl_follows_the_configured_duration(
    catalog, monkeypatch, configured,
):
    cat, client = catalog
    _set_ttl(monkeypatch, configured)

    token = cat.acquire_simple_lock("org", "lake", "orders")

    assert token
    assert client.ttl(RK.lock_leaf("org", "lake", "orders")) == configured


def test_namespace_and_stage_locks_follow_the_same_duration(
    catalog, monkeypatch,
):
    cat, client = catalog
    _set_ttl(monkeypatch, 17)

    assert cat.acquire_namespace_lock("org", "lake")
    assert cat.acquire_stage_lock("org", "lake", "landing")

    assert client.ttl(RK.lock_namespace("org", "lake")) == 17
    assert client.ttl(RK.lock_stage("org", "lake", "landing")) == 17


def test_an_explicit_ttl_still_overrides_the_configured_default(
    catalog, monkeypatch,
):
    """Operations with a deliberate recovery window keep controlling it."""
    cat, client = catalog
    _set_ttl(monkeypatch, 30)

    assert cat.acquire_simple_lock("org", "lake", "orders", ttl_s=90)

    assert client.ttl(RK.lock_leaf("org", "lake", "orders")) == 90


@pytest.mark.parametrize("bad", [0, -1, 2.5, True, "30"])
def test_an_invalid_ttl_is_rejected_rather_than_coerced(catalog, bad):
    cat, _client = catalog
    with pytest.raises(ValueError):
        cat.acquire_simple_lock("org", "lake", "orders", ttl_s=bad)


def test_the_writer_does_not_hardcode_a_lock_ttl():
    """Guard the exact regression: a literal TTL at a lock call site.

    ``data_writer`` must inherit the configured duration.  A literal
    ``ttl_s=<number>`` here is what made the setting inert for every release
    up to 2.6.0.
    """
    import inspect
    import re

    from supertable import data_writer

    source = inspect.getsource(data_writer)
    offenders = re.findall(r"ttl_s\s*=\s*\d+", source)
    assert not offenders, (
        "data_writer hardcodes a lock TTL and so ignores "
        f"DEFAULT_LOCK_DURATION_SEC: {offenders}"
    )
