"""Regression coverage for the audit Redis connection authority boundary."""
from __future__ import annotations

import importlib
import sys
from dataclasses import replace
from types import SimpleNamespace

import pytest
import redis

from supertable import redis_catalog, redis_connector


def _connector_settings(**overrides):
    values = {
        "SUPERTABLE_REDIS_URL": "",
        "SUPERTABLE_REDIS_HOST": "redis.example",
        "SUPERTABLE_REDIS_PORT": 6379,
        "SUPERTABLE_REDIS_DB": 0,
        "SUPERTABLE_REDIS_USERNAME": "audit-writer",
        "SUPERTABLE_REDIS_PASSWORD": "secret",
        "SUPERTABLE_REDIS_SSL": False,
        "SUPERTABLE_REDIS_SSL_CA_CERTS": "",
        "SUPERTABLE_REDIS_SENTINEL": False,
        "SUPERTABLE_REDIS_SENTINELS": "",
        "SUPERTABLE_REDIS_SENTINEL_MASTER": "mymaster",
        "effective_redis_sentinel_password": "sentinel-secret",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.fixture
def redis_infra(monkeypatch):
    """Import the eager legacy facade without opening a network connection."""

    client = SimpleNamespace(register_script=lambda _script: object())
    original_create_redis_client = redis_connector.create_redis_client
    settings_module = importlib.import_module("supertable.config.settings")
    monkeypatch.setattr(
        settings_module,
        "settings",
        replace(
            settings_module.settings,
            SUPERTABLE_ORGANIZATION="test-org",
            # A Redis-only/MCP process must not receive the SuperTable API's
            # global superuser credential merely to construct its Redis client.
            SUPERTABLE_SUPERUSER_TOKEN="",
        ),
    )

    class Catalog:
        def __init__(self, options=None, *, redis_client=None):
            assert options is None
            self.r = redis_client

    monkeypatch.setattr(redis_connector, "settings", _connector_settings())
    monkeypatch.setattr(redis_connector, "create_redis_client", lambda _opts=None: client)
    monkeypatch.setattr(redis_catalog, "RedisCatalog", Catalog)
    sys.modules.pop("supertable.redis_infra", None)
    module = importlib.import_module("supertable.redis_infra")
    # Subsequent calls under test must exercise the real hardened builder; the
    # stub above exists only to make the module's eager compatibility exports
    # safe to import.
    monkeypatch.setattr(
        redis_connector, "create_redis_client", original_create_redis_client,
    )
    yield module
    sys.modules.pop("supertable.redis_infra", None)


def test_catalog_and_public_client_share_hardened_connection(
    redis_infra, monkeypatch,
):
    client = object()

    class Catalog:
        def __init__(self, options=None, *, redis_client=None):
            assert options is None
            self.r = redis_client

    monkeypatch.setattr(redis_infra, "_build_redis_client", lambda: client)
    monkeypatch.setattr(redis_catalog, "RedisCatalog", Catalog)

    catalog, exposed = redis_infra._build_catalog()

    assert exposed is client
    assert catalog.r is client


def test_runtime_redis_env_requires_organization_not_superuser_api_token(
    redis_infra, monkeypatch,
):
    redis_infra._require_runtime_env()

    monkeypatch.setattr(
        redis_infra.settings, "SUPERTABLE_ORGANIZATION", "",
    )
    with pytest.raises(RuntimeError, match="SUPERTABLE_ORGANIZATION") as exc_info:
        redis_infra._require_runtime_env()
    assert "SUPERTABLE_SUPERUSER_TOKEN" not in str(exc_info.value)


def test_split_tls_uses_required_certificate_and_hostname_verification(
    redis_infra, monkeypatch,
):
    captured = {}
    client = object()
    monkeypatch.setattr(
        redis_connector,
        "settings",
        _connector_settings(SUPERTABLE_REDIS_SSL=True),
    )
    monkeypatch.setattr(
        redis_connector.redis,
        "Redis",
        lambda **kwargs: captured.update(kwargs) or client,
    )
    monkeypatch.setattr(redis_infra, "_require_runtime_env", lambda: None)
    redis_connector._CLIENT_CACHE.clear()

    assert redis_infra._build_redis_client() is client
    assert captured["ssl"] is True
    assert captured["ssl_cert_reqs"] == "required"
    assert captured["ssl_check_hostname"] is True


def test_malformed_sentinel_never_degrades_to_direct_redis(
    redis_infra, monkeypatch,
):
    direct_calls = []
    monkeypatch.setattr(
        redis_connector,
        "settings",
        _connector_settings(
            SUPERTABLE_REDIS_SENTINEL=True,
            SUPERTABLE_REDIS_SENTINELS="sentinel-a:26379,broken",
        ),
    )
    monkeypatch.setattr(
        redis_connector.redis,
        "Redis",
        lambda **kwargs: direct_calls.append(kwargs) or object(),
    )
    monkeypatch.setattr(redis_infra, "_require_runtime_env", lambda: None)
    redis_connector._CLIENT_CACHE.clear()

    with pytest.raises(ValueError, match="SUPERTABLE_REDIS_SENTINELS"):
        redis_infra._build_redis_client()
    assert direct_calls == []


def test_unavailable_sentinel_never_falls_back_to_direct_redis(
    redis_infra, monkeypatch,
):
    direct_calls = []

    class UnavailableMaster:
        def ping(self):
            raise redis.ConnectionError("sentinel unavailable")

    class Sentinel:
        def __init__(self, *_args, **_kwargs):
            pass

        def master_for(self, *_args, **_kwargs):
            return UnavailableMaster()

    moments = iter((0.0, 0.0, 4.0))
    monkeypatch.setattr(
        redis_connector,
        "settings",
        _connector_settings(
            SUPERTABLE_REDIS_SENTINEL=True,
            SUPERTABLE_REDIS_SENTINELS="sentinel-a:26379",
        ),
    )
    monkeypatch.setattr(redis_connector, "Sentinel", Sentinel)
    monkeypatch.setattr(
        redis_connector,
        "time",
        SimpleNamespace(
            time=lambda: next(moments),
            sleep=lambda _seconds: None,
        ),
    )
    monkeypatch.setattr(
        redis_connector.redis,
        "Redis",
        lambda **kwargs: direct_calls.append(kwargs) or object(),
    )
    monkeypatch.setattr(redis_infra, "_require_runtime_env", lambda: None)
    redis_connector._CLIENT_CACHE.clear()

    with pytest.raises(redis.ConnectionError, match="sentinel unavailable"):
        redis_infra._build_redis_client()
    assert direct_calls == []
