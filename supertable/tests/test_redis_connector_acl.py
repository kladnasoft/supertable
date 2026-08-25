"""Redis ACL identity propagation for direct and Sentinel connections."""
from __future__ import annotations

from types import SimpleNamespace
import traceback

import pytest

from supertable import redis_connector


def _options(**overrides):
    values = {
        "host": "redis.example",
        "port": 6379,
        "db": 3,
        "username": "supertable-audited-writer",
        "password": "secret",
        "use_ssl": True,
        "is_sentinel": False,
        "sentinel_hosts": [],
        "sentinel_master": "mymaster",
        "sentinel_password": "sentinel-secret",
        "sentinel_strict": True,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _settings(**overrides):
    values = {
        "SUPERTABLE_REDIS_URL": "",
        "SUPERTABLE_REDIS_HOST": "split.example",
        "SUPERTABLE_REDIS_PORT": 6379,
        "SUPERTABLE_REDIS_DB": 0,
        "SUPERTABLE_REDIS_USERNAME": "split-user",
        "SUPERTABLE_REDIS_PASSWORD": "split-password",
        "SUPERTABLE_REDIS_SSL": False,
        "SUPERTABLE_REDIS_SENTINEL": False,
        "SUPERTABLE_REDIS_SENTINELS": "",
        "SUPERTABLE_REDIS_SENTINEL_MASTER": "mymaster",
        "effective_redis_sentinel_password": "sentinel-password",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_direct_redis_connection_receives_acl_username(monkeypatch):
    captured = {}
    client = object()

    def fake_redis(**kwargs):
        captured.update(kwargs)
        return client

    monkeypatch.setattr(redis_connector.redis, "Redis", fake_redis)

    assert redis_connector._build_redis_client(_options(), True) is client
    assert captured["username"] == "supertable-audited-writer"
    assert captured["password"] == "secret"


@pytest.mark.parametrize("use_ssl", [False, True])
def test_sentinel_connections_receive_tls_and_acl_credentials(monkeypatch, use_ssl):
    sentinel_kwargs = {}
    master_kwargs = {}
    master = SimpleNamespace(ping=lambda: True)

    class FakeSentinel:
        def __init__(self, hosts, **kwargs):
            sentinel_kwargs["hosts"] = hosts
            sentinel_kwargs.update(kwargs)

        def master_for(self, name, **kwargs):
            master_kwargs["name"] = name
            master_kwargs.update(kwargs)
            return master

    monkeypatch.setattr(redis_connector, "Sentinel", FakeSentinel)
    options = _options(
        is_sentinel=True,
        sentinel_hosts=[("sentinel-a", 26379)],
        use_ssl=use_ssl,
    )

    assert redis_connector._build_redis_client(options, True) is master
    discovery_kwargs = sentinel_kwargs["sentinel_kwargs"]
    assert discovery_kwargs["username"] == "supertable-audited-writer"
    assert discovery_kwargs["password"] == "sentinel-secret"
    assert discovery_kwargs["ssl"] is use_ssl
    assert sentinel_kwargs["username"] == "supertable-audited-writer"
    assert sentinel_kwargs["password"] == "secret"
    assert sentinel_kwargs["ssl"] is use_ssl
    assert master_kwargs["username"] == "supertable-audited-writer"
    assert master_kwargs["password"] == "secret"
    assert master_kwargs["ssl"] is use_ssl


def test_client_cache_identity_includes_acl_username():
    first = redis_connector._options_cache_key(_options(username="writer-a"))
    second = redis_connector._options_cache_key(_options(username="writer-b"))
    assert first != second


def test_direct_redis_url_takes_precedence_and_decodes_acl_credentials(monkeypatch):
    monkeypatch.setattr(
        redis_connector,
        "settings",
        _settings(
            SUPERTABLE_REDIS_URL=(
                "rediss://audit%2Dwriter:p%40ss%2Fword@secure.example:6381/7"
            ),
            SUPERTABLE_REDIS_HOST="ignored.example",
            SUPERTABLE_REDIS_PORT=6399,
            SUPERTABLE_REDIS_DB=12,
            SUPERTABLE_REDIS_USERNAME="ignored-user",
            SUPERTABLE_REDIS_PASSWORD="ignored-password",
            SUPERTABLE_REDIS_SSL=False,
        ),
    )

    options = redis_connector.RedisOptions()

    assert options.host == "secure.example"
    assert options.port == 6381
    assert options.db == 7
    assert options.username == "audit-writer"
    assert options.password == "p@ss/word"
    assert options.use_ssl is True


def test_direct_redis_url_defaults_port_and_database(monkeypatch):
    monkeypatch.setattr(
        redis_connector,
        "settings",
        _settings(
            SUPERTABLE_REDIS_URL="redis://cache.example",
            SUPERTABLE_REDIS_PORT=6399,
            SUPERTABLE_REDIS_DB=12,
            SUPERTABLE_REDIS_SSL=True,
        ),
    )

    options = redis_connector.RedisOptions()

    assert options.port == 6379
    assert options.db == 0
    assert options.username is None
    assert options.password is None
    assert options.use_ssl is False


@pytest.mark.parametrize(
    "redis_url",
    [
        "http://redis.example/0",
        "redis:///0",
        "redis://redis.example/not-a-db",
        "redis://redis.example/1/extra",
        "redis://redis.example:0/0",
        "redis://redis.example:bad/0",
        "redis://user%ZZ:password@redis.example/0",
        "redis://user:%FF@redis.example/0",
        "redis://redis.example/0?ssl=true",
        "redis://redis.example/0#fragment",
    ],
)
def test_malformed_or_unsupported_direct_redis_url_fails_closed(
    monkeypatch, redis_url
):
    monkeypatch.setattr(
        redis_connector,
        "settings",
        _settings(SUPERTABLE_REDIS_URL=redis_url),
    )

    with pytest.raises(ValueError, match="SUPERTABLE_REDIS_URL"):
        redis_connector.RedisOptions()


@pytest.mark.parametrize(
    "redis_url",
    [
        "redis://user:REDIS_PASSWORD_DO_NOT_LOG@[broken/0",
        "redis://user:REDIS_PASSWORD_DO_NOT_LOG@redis.example:not-a-port/0",
        "redis://user:%FF@redis.example/0",
    ],
)
def test_malformed_direct_redis_url_never_reflects_credentials(redis_url):
    with pytest.raises(ValueError) as caught:
        redis_connector._parse_direct_redis_url(redis_url)

    rendered = "".join(
        traceback.format_exception(
            type(caught.value), caught.value, caught.value.__traceback__,
        )
    )
    assert "REDIS_PASSWORD_DO_NOT_LOG" not in rendered
    assert "%FF" not in rendered


def test_sentinel_mode_retains_split_connection_contract(monkeypatch):
    monkeypatch.setattr(
        redis_connector,
        "settings",
        _settings(
            SUPERTABLE_REDIS_URL="unsupported://ignored-in-sentinel-mode",
            SUPERTABLE_REDIS_HOST="sentinel-fallback.example",
            SUPERTABLE_REDIS_PORT=6388,
            SUPERTABLE_REDIS_DB=4,
            SUPERTABLE_REDIS_USERNAME="sentinel-master-user",
            SUPERTABLE_REDIS_PASSWORD="sentinel-master-password",
            SUPERTABLE_REDIS_SSL=True,
            SUPERTABLE_REDIS_SENTINEL=True,
            SUPERTABLE_REDIS_SENTINELS="sentinel-a:26379,sentinel-b:26380",
        ),
    )

    options = redis_connector.RedisOptions()

    assert options.host == "sentinel-fallback.example"
    assert options.port == 6388
    assert options.db == 4
    assert options.username == "sentinel-master-user"
    assert options.password == "sentinel-master-password"
    assert options.use_ssl is True
    assert options.sentinel_hosts == [
        ("sentinel-a", 26379),
        ("sentinel-b", 26380),
    ]


@pytest.mark.parametrize(
    "sentinel_list",
    [
        "",
        "   ",
        "sentinel-without-port",
        ":26379",
        "sentinel-a:not-a-port",
        "sentinel-a:0",
        "sentinel-a:65536",
        "sentinel-a:26379,broken",
        "sentinel-a:26379,,sentinel-b:26379",
    ],
)
def test_enabled_sentinel_rejects_empty_or_partially_malformed_endpoint_set(
    monkeypatch, sentinel_list,
):
    monkeypatch.setattr(
        redis_connector,
        "settings",
        _settings(
            SUPERTABLE_REDIS_SENTINEL=True,
            SUPERTABLE_REDIS_SENTINELS=sentinel_list,
        ),
    )

    with pytest.raises(ValueError, match="SUPERTABLE_REDIS_SENTINELS"):
        redis_connector.RedisOptions()


def test_enabled_sentinel_rejects_empty_master_name(monkeypatch):
    monkeypatch.setattr(
        redis_connector,
        "settings",
        _settings(
            SUPERTABLE_REDIS_SENTINEL=True,
            SUPERTABLE_REDIS_SENTINELS="sentinel-a:26379",
            SUPERTABLE_REDIS_SENTINEL_MASTER="",
        ),
    )

    with pytest.raises(ValueError, match="SUPERTABLE_REDIS_SENTINEL_MASTER"):
        redis_connector.RedisOptions()


def test_sentinel_builder_never_falls_back_to_standalone_for_empty_endpoints(
    monkeypatch,
):
    direct_calls = []
    monkeypatch.setattr(
        redis_connector.redis,
        "Redis",
        lambda **kwargs: direct_calls.append(kwargs) or object(),
    )

    with pytest.raises(ValueError, match="at least one validated endpoint"):
        redis_connector._build_redis_client(
            _options(is_sentinel=True, sentinel_hosts=[]), True,
        )
    assert direct_calls == []


def test_client_cache_identity_reflects_url_derived_endpoint(monkeypatch):
    monkeypatch.setattr(
        redis_connector,
        "settings",
        _settings(SUPERTABLE_REDIS_URL="redis://writer-a:one@redis-a:6379/1"),
    )
    first = redis_connector.RedisOptions()
    monkeypatch.setattr(
        redis_connector,
        "settings",
        _settings(SUPERTABLE_REDIS_URL="rediss://writer-b:two@redis-b:6380/2"),
    )
    second = redis_connector.RedisOptions()

    assert redis_connector._options_cache_key(first) != redis_connector._options_cache_key(
        second
    )


def test_direct_client_has_bounded_connect_and_command_timeouts(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        redis_connector.redis,
        "Redis",
        lambda **kwargs: captured.update(kwargs) or object(),
    )
    redis_connector._build_redis_client(_options(), True)
    assert captured["socket_timeout"] == 0.5
    assert captured["socket_connect_timeout"] == 0.5
