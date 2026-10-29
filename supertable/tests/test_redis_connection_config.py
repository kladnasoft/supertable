"""How the Redis client builders read their settings.

There are two builders: ``redis_connector`` (catalog, writer, locks) and
``redis_infra`` (audit and the platform layer). Both must reach the same
server with the same TLS and credentials from the same settings. This file
also covers the refusal of a cluster-mode Redis, such as Azure Managed Redis
with its default OSS clustering policy.
"""
from __future__ import annotations

import dataclasses
import importlib
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import redis

import supertable.config.settings as cfg
import supertable.redis_connector as rc


def _connector_settings(**over):
    base = dict(
        SUPERTABLE_REDIS_HOST="localhost", SUPERTABLE_REDIS_PORT=6379, SUPERTABLE_REDIS_DB=0,
        SUPERTABLE_REDIS_USERNAME="", SUPERTABLE_REDIS_PASSWORD="", SUPERTABLE_REDIS_SSL=False,
        SUPERTABLE_REDIS_URL="", SUPERTABLE_REDIS_SENTINEL=False, SUPERTABLE_REDIS_SENTINELS="",
        SUPERTABLE_REDIS_SENTINEL_MASTER="mymaster", effective_redis_sentinel_password="",
    )
    base.update(over)
    return SimpleNamespace(**base)


def _options(monkeypatch, **over) -> rc.RedisOptions:
    monkeypatch.setattr(rc, "settings", _connector_settings(**over))
    return rc.RedisOptions()


# --------------------------------------------------------------------------- #
# redis_connector: options
# --------------------------------------------------------------------------- #

def test_separate_settings_unchanged(monkeypatch):
    o = _options(monkeypatch, SUPERTABLE_REDIS_HOST="h", SUPERTABLE_REDIS_PORT=10000,
                 SUPERTABLE_REDIS_DB=2, SUPERTABLE_REDIS_PASSWORD="pw", SUPERTABLE_REDIS_SSL=True)
    assert (o.host, o.port, o.db, o.username, o.password, o.use_ssl) == ("h", 10000, 2, None, "pw", True)


def test_url_gives_server_credentials_and_tls(monkeypatch):
    # An Azure access key holds '/', '+' and '='; '/' must be percent-encoded in a URL.
    o = _options(monkeypatch, SUPERTABLE_REDIS_URL="rediss://default:ab%2Fc+d%3D@x.redis.azure.net:10000/0")
    assert (o.host, o.port, o.db) == ("x.redis.azure.net", 10000, 0)
    assert (o.username, o.password, o.use_ssl) == ("default", "ab/c+d=", True)


def test_url_scheme_decides_tls(monkeypatch):
    o = _options(monkeypatch, SUPERTABLE_REDIS_URL="redis://h:6379", SUPERTABLE_REDIS_SSL=True)
    assert o.use_ssl is False


def test_url_without_password_keeps_the_separate_one(monkeypatch):
    o = _options(monkeypatch, SUPERTABLE_REDIS_URL="rediss://h:10000", SUPERTABLE_REDIS_PASSWORD="pw")
    assert (o.host, o.password, o.db) == ("h", "pw", 0)


def test_unsupported_url_scheme_raises(monkeypatch):
    with pytest.raises(ValueError):
        _options(monkeypatch, SUPERTABLE_REDIS_URL="http://h:6379")


def test_username_without_password_is_dropped(monkeypatch):
    assert _options(monkeypatch, SUPERTABLE_REDIS_USERNAME="app").username is None
    assert _options(monkeypatch, SUPERTABLE_REDIS_USERNAME="app",
                    SUPERTABLE_REDIS_PASSWORD="pw").username == "app"


def test_username_separates_cached_clients(monkeypatch):
    a = _options(monkeypatch, SUPERTABLE_REDIS_USERNAME="a", SUPERTABLE_REDIS_PASSWORD="pw")
    b = _options(monkeypatch, SUPERTABLE_REDIS_USERNAME="b", SUPERTABLE_REDIS_PASSWORD="pw")
    assert rc._options_cache_key(a) != rc._options_cache_key(b)


# --------------------------------------------------------------------------- #
# redis_connector: client construction and cluster mode
# --------------------------------------------------------------------------- #

def _build(monkeypatch, info, **over):
    client = MagicMock(name="redis-client")
    if isinstance(info, BaseException):
        client.info.side_effect = info
    else:
        client.info.return_value = info
    ctor = MagicMock(return_value=client)
    monkeypatch.setattr(rc.redis, "Redis", ctor)
    return rc._build_redis_client(_options(monkeypatch, **over), True), ctor, client


def test_client_gets_username_password_and_tls(monkeypatch):
    built, ctor, client = _build(monkeypatch, {"cluster_enabled": 0},
                                 SUPERTABLE_REDIS_URL="rediss://app:pw@h:10000/0")
    assert built is client
    kw = ctor.call_args.kwargs
    assert (kw["host"], kw["port"], kw["username"], kw["password"], kw["ssl"]) == ("h", 10000, "app", "pw", True)


def test_cluster_mode_is_refused(monkeypatch):
    with pytest.raises(rc.RedisClusterModeError, match="NoCluster"):
        _build(monkeypatch, {"cluster_enabled": 1})


@pytest.mark.parametrize("failure", [
    redis.ConnectionError("refused"), redis.AuthenticationError("bad key"), redis.ResponseError("unknown section"),
])
def test_probe_failure_leaves_the_error_to_the_first_command(monkeypatch, failure):
    built, _, client = _build(monkeypatch, failure)
    assert built is client


# --------------------------------------------------------------------------- #
# redis_infra: same reading of the settings
# --------------------------------------------------------------------------- #

@pytest.fixture
def infra(monkeypatch):
    """redis_infra imported against stub settings and no network.

    Importing it builds a catalog and a client, so the import itself runs
    under the patches; the module that was loaded before is restored after.
    """
    name = "supertable.redis_infra"
    previous = sys.modules.pop(name, None)
    base = dataclasses.replace(
        cfg.settings, SUPERTABLE_ORGANIZATION="acme", SUPERTABLE_SUPERUSER_TOKEN="t",
        SUPERTABLE_LOGIN_MASK=1, SUPERTABLE_REDIS_URL="", SUPERTABLE_REDIS_SENTINEL=False,
        SUPERTABLE_REDIS_USERNAME="", SUPERTABLE_REDIS_PASSWORD="", SUPERTABLE_REDIS_SSL=False,
    )
    monkeypatch.setattr(cfg, "settings", base)
    ctor = MagicMock()
    monkeypatch.setattr(redis, "Redis", ctor)
    try:
        module = importlib.import_module(name)
        ctor.reset_mock()

        def build(**over):
            monkeypatch.setattr(module, "_cfg", dataclasses.replace(base, **over))
            module._build_redis_client()
            return ctor.call_args.kwargs

        yield build
    finally:
        sys.modules.pop(name, None)
        if previous is not None:
            sys.modules[name] = previous


def test_infra_honours_the_ssl_setting(infra):
    # Azure Managed Redis accepts only TLS; the audit client must use it too.
    assert infra(SUPERTABLE_REDIS_HOST="h", SUPERTABLE_REDIS_PORT=10000,
                 SUPERTABLE_REDIS_PASSWORD="pw", SUPERTABLE_REDIS_SSL=True)["ssl"] is True
    assert infra(SUPERTABLE_REDIS_SSL=False)["ssl"] is False


def test_infra_url_scheme_decides_tls_and_credentials_are_decoded(infra):
    kw = infra(SUPERTABLE_REDIS_URL="redis://default:ab%2Fc@h:6379/0", SUPERTABLE_REDIS_SSL=True)
    assert (kw["ssl"], kw["password"]) == (False, "ab/c")
    assert infra(SUPERTABLE_REDIS_URL="rediss://:pw@h:10000")["ssl"] is True
