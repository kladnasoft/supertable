# route: supertable.redis_connector
from __future__ import annotations

import os
import string
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote_to_bytes, urlsplit


import redis
from redis.sentinel import MasterNotFoundError, Sentinel
from supertable.config.defaults import logger
from supertable.config.settings import settings



@dataclass(frozen=True)
class RedisOptions:
    """
    Reads Redis connection options from environment variables.

    Supported:
      - SUPERTABLE_REDIS_URL (direct mode; redis:// or rediss://; takes precedence)
      - SUPERTABLE_REDIS_HOST (default: localhost)
      - SUPERTABLE_REDIS_PORT (default: 6379)
      - SUPERTABLE_REDIS_DB (default: 0)
      - SUPERTABLE_REDIS_USERNAME (Redis ACL username; optional)
      - SUPERTABLE_REDIS_PASSWORD
      - SUPERTABLE_REDIS_SSL (default: false)

    Hardcoded (not configurable):
      - decode_responses is always True (all Redis data is JSON text)
      - sentinel_strict is always True (no silent fallback to standalone Redis)

    Sentinel (optional):
      - SUPERTABLE_REDIS_SENTINEL (true/false)
      - SUPERTABLE_REDIS_SENTINELS="host1:26379,host2:26379"
      - SUPERTABLE_REDIS_SENTINEL_MASTER="mymaster"
      - SUPERTABLE_REDIS_SENTINEL_PASSWORD (optional; defaults to SUPERTABLE_REDIS_PASSWORD; also used as fallback for SUPERTABLE_REDIS_PASSWORD in Sentinel mode)
    """
    host: str = field(init=False)
    port: int = field(init=False)
    db: int = field(init=False)
    username: Optional[str] = field(init=False)
    password: Optional[str] = field(init=False)
    use_ssl: bool = field(init=False)
    ssl_ca_certs: Optional[str] = field(init=False)
    decode_responses: bool = field(default=False)

    # Sentinel-related
    is_sentinel: bool = field(init=False)
    sentinel_hosts: List[tuple] = field(init=False)
    sentinel_master: str = field(init=False)
    sentinel_password: Optional[str] = field(init=False)
    sentinel_strict: bool = field(init=False)

    def __post_init__(self):
        host = settings.SUPERTABLE_REDIS_HOST
        port = settings.SUPERTABLE_REDIS_PORT
        db = settings.SUPERTABLE_REDIS_DB
        username = settings.SUPERTABLE_REDIS_USERNAME or None
        password = settings.SUPERTABLE_REDIS_PASSWORD or None
        use_ssl = settings.SUPERTABLE_REDIS_SSL
        ssl_ca_certs = (
            getattr(settings, "SUPERTABLE_REDIS_SSL_CA_CERTS", "") or ""
        ).strip() or None

        host = _validate_redis_host(host, field_name="SUPERTABLE_REDIS_HOST")
        port = _validate_redis_port(port, field_name="SUPERTABLE_REDIS_PORT")
        db = _validate_redis_db(db, field_name="SUPERTABLE_REDIS_DB")
        if not isinstance(use_ssl, bool):
            raise ValueError("SUPERTABLE_REDIS_SSL must be a boolean")

        # All Redis data is JSON text — always decode responses to str.
        decode = True

        # Sentinel mode configuration
        is_sentinel = settings.SUPERTABLE_REDIS_SENTINEL
        if not isinstance(is_sentinel, bool):
            raise ValueError("SUPERTABLE_REDIS_SENTINEL must be a boolean")
        sentinel_raw = settings.SUPERTABLE_REDIS_SENTINELS
        # Sentinel discovery is an authority boundary: when it is enabled an
        # incomplete endpoint list must never degrade into a direct Redis
        # connection.  Ignore an inactive stale list, but validate every
        # configured endpoint when Sentinel mode is selected.
        sentinels = (
            _parse_sentinel_hosts(sentinel_raw)
            if is_sentinel
            else []
        )

        sentinel_master = settings.SUPERTABLE_REDIS_SENTINEL_MASTER
        if is_sentinel:
            _validate_sentinel_identity(
                sentinel_master,
                field_name="SUPERTABLE_REDIS_SENTINEL_MASTER",
            )

        sentinel_password = settings.effective_redis_sentinel_password or None

        # A Redis URL is an authoritative direct-connection identity.  Do not
        # combine URL credentials or TLS state with split settings: doing so can
        # silently authenticate to a different endpoint than the operator
        # configured.  Sentinel discovery keeps its existing split-variable
        # contract because a single Redis URL cannot describe the Sentinel set.
        redis_url = (settings.SUPERTABLE_REDIS_URL or "").strip()
        if redis_url and not is_sentinel:
            host, port, db, username, password, use_ssl = _parse_direct_redis_url(
                redis_url
            )

        if ssl_ca_certs is not None:
            if not use_ssl:
                raise ValueError(
                    "SUPERTABLE_REDIS_SSL_CA_CERTS requires Redis TLS"
                )
            if (
                not os.path.isfile(ssl_ca_certs)
                or not os.access(ssl_ca_certs, os.R_OK)
            ):
                raise ValueError(
                    "SUPERTABLE_REDIS_SSL_CA_CERTS must name a readable CA file"
                )

        # In many deployments the Sentinel and Redis server share the same password.
        # Allow a single-password setup by reusing SUPERTABLE_REDIS_SENTINEL_PASSWORD
        # for Redis auth when SUPERTABLE_REDIS_PASSWORD is not set.
        if is_sentinel and password is None and sentinel_password:
            password = sentinel_password

        # Never silently fall back to standalone Redis — fail loud in production.
        sentinel_strict = True

        object.__setattr__(self, "host", host)
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "db", db)
        object.__setattr__(self, "username", username)
        object.__setattr__(self, "password", password)
        object.__setattr__(self, "use_ssl", use_ssl)
        object.__setattr__(self, "ssl_ca_certs", ssl_ca_certs)
        object.__setattr__(self, "decode_responses", decode)

        object.__setattr__(self, "is_sentinel", is_sentinel)
        object.__setattr__(self, "sentinel_hosts", sentinels)
        object.__setattr__(self, "sentinel_master", sentinel_master)
        object.__setattr__(self, "sentinel_password", sentinel_password)
        object.__setattr__(self, "sentinel_strict", sentinel_strict)


def _validate_redis_host(value: Any, *, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 253
        or any(
            character.isspace()
            or ord(character) < 32
            or ord(character) == 127
            for character in value
        )
    ):
        raise ValueError(f"{field_name} must contain a valid non-empty host")
    return value


def _validate_redis_port(value: Any, *, field_name: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= 65535
    ):
        raise ValueError(f"{field_name} must be an integer between 1 and 65535")
    return value


def _validate_redis_db(value: Any, *, field_name: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= 2**31 - 1
    ):
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _validate_sentinel_identity(value: Any, *, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 253
        or any(
            character.isspace()
            or ord(character) < 32
            or ord(character) == 127
            for character in value
        )
    ):
        raise ValueError(f"{field_name} must contain a valid non-empty name")
    return value


def _parse_sentinel_hosts(value: Any) -> List[Tuple[str, int]]:
    """Parse an all-or-nothing Sentinel endpoint set without logging its input."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            "SUPERTABLE_REDIS_SENTINELS must contain at least one host:port "
            "endpoint when Sentinel mode is enabled"
        )
    endpoints: List[Tuple[str, int]] = []
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part or ":" not in part:
            raise ValueError(
                "SUPERTABLE_REDIS_SENTINELS contains a malformed host:port endpoint"
            )
        raw_host, raw_port = part.rsplit(":", 1)
        host = raw_host.strip()
        port_text = raw_port.strip()
        # Accept conventional bracketed IPv6 while passing the unbracketed
        # address expected by redis-py.
        if host.startswith("[") or host.endswith("]"):
            if not (host.startswith("[") and host.endswith("]")):
                raise ValueError(
                    "SUPERTABLE_REDIS_SENTINELS contains a malformed host:port endpoint"
                )
            host = host[1:-1]
        _validate_sentinel_identity(
            host, field_name="SUPERTABLE_REDIS_SENTINELS host",
        )
        if (
            not port_text
            or not port_text.isascii()
            or not port_text.isdecimal()
        ):
            raise ValueError(
                "SUPERTABLE_REDIS_SENTINELS contains a non-numeric port"
            )
        port = int(port_text)
        if not 1 <= port <= 65535:
            raise ValueError(
                "SUPERTABLE_REDIS_SENTINELS port must be between 1 and 65535"
            )
        endpoints.append((host, port))
    return endpoints


def _validated_sentinel_hosts(value: Any) -> List[Tuple[str, int]]:
    """Defence-in-depth for callers that construct options without RedisOptions."""

    if not isinstance(value, (tuple, list)) or not value:
        raise ValueError(
            "Redis Sentinel mode requires at least one validated endpoint"
        )
    endpoints: List[Tuple[str, int]] = []
    for endpoint in value:
        if not isinstance(endpoint, (tuple, list)) or len(endpoint) != 2:
            raise ValueError("Redis Sentinel endpoint must be a (host, port) pair")
        host, port = endpoint
        host = _validate_sentinel_identity(
            host, field_name="Redis Sentinel host",
        )
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("Redis Sentinel port must be between 1 and 65535")
        endpoints.append((host, port))
    return endpoints


def _decode_url_credential(value: Optional[str], *, field_name: str) -> Optional[str]:
    """Strictly percent-decode one Redis URL credential as UTF-8 text."""

    if value is None:
        return None
    index = 0
    while index < len(value):
        if value[index] == "%":
            if (
                index + 2 >= len(value)
                or value[index + 1] not in string.hexdigits
                or value[index + 2] not in string.hexdigits
            ):
                raise ValueError(
                    f"SUPERTABLE_REDIS_URL has an invalid percent escape in {field_name}"
                )
            index += 3
            continue
        index += 1
    try:
        decoded = unquote_to_bytes(value).decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"SUPERTABLE_REDIS_URL {field_name} is not valid UTF-8"
        ) from exc
    return decoded or None


def _parse_direct_redis_url(
    value: str,
) -> Tuple[str, int, int, Optional[str], Optional[str], bool]:
    """Parse the supported, security-relevant subset of a direct Redis URL."""

    try:
        parsed = urlsplit(value)
    except ValueError as exc:
        raise ValueError(f"Invalid SUPERTABLE_REDIS_URL: {exc}") from exc
    scheme = parsed.scheme.lower()
    if scheme not in {"redis", "rediss"}:
        raise ValueError(
            "SUPERTABLE_REDIS_URL scheme must be 'redis' or 'rediss'"
        )
    if parsed.query or parsed.fragment:
        raise ValueError(
            "SUPERTABLE_REDIS_URL query parameters and fragments are not supported"
        )
    try:
        host = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"Invalid SUPERTABLE_REDIS_URL: {exc}") from exc
    host = _validate_redis_host(
        host, field_name="SUPERTABLE_REDIS_URL host",
    )
    if port is None:
        port = 6379
    if not 1 <= port <= 65535:
        raise ValueError("SUPERTABLE_REDIS_URL port must be between 1 and 65535")

    if parsed.path in {"", "/"}:
        db = 0
    else:
        raw_db = parsed.path[1:] if parsed.path.startswith("/") else ""
        if not raw_db.isdecimal():
            raise ValueError(
                "SUPERTABLE_REDIS_URL path must contain one non-negative database number"
            )
        db = int(raw_db)
        if db > 2**31 - 1:
            raise ValueError(
                "SUPERTABLE_REDIS_URL database number is out of range"
            )

    username = _decode_url_credential(parsed.username, field_name="username")
    password = _decode_url_credential(parsed.password, field_name="password")
    return host, port, db, username, password, scheme == "rediss"


# Process-level cache: identical RedisOptions → identical client (and pool).
# Multiple writer/catalog instances share one ConnectionPool instead of each
# constructing a new redis.Redis (which would leak its own pool until GC).
_CLIENT_CACHE: Dict[Tuple[Any, ...], redis.Redis] = {}
_CLIENT_CACHE_LOCK = threading.Lock()


def _options_cache_key(opts: RedisOptions) -> Tuple[Any, ...]:
    return (
        opts.host,
        opts.port,
        opts.db,
        opts.username,
        opts.password,
        opts.use_ssl,
        getattr(opts, "ssl_ca_certs", None),
        opts.is_sentinel,
        tuple(opts.sentinel_hosts),
        opts.sentinel_master,
        opts.sentinel_password,
        opts.sentinel_strict,
    )


def create_redis_client(options: Optional[RedisOptions] = None) -> redis.Redis:
    opts = options or RedisOptions()
    decode_responses = True

    cache_key = _options_cache_key(opts)
    cached = _CLIENT_CACHE.get(cache_key)
    if cached is not None:
        return cached

    with _CLIENT_CACHE_LOCK:
        cached = _CLIENT_CACHE.get(cache_key)
        if cached is not None:
            return cached
        client = _build_redis_client(opts, decode_responses)
        _CLIENT_CACHE[cache_key] = client
        return client


def _build_redis_client(opts: RedisOptions, decode_responses: bool) -> redis.Redis:
    if not isinstance(opts.use_ssl, bool):
        raise ValueError("Redis TLS mode must be a boolean")
    if not isinstance(opts.is_sentinel, bool):
        raise ValueError("Redis Sentinel mode must be a boolean")
    _validate_redis_db(opts.db, field_name="Redis database")
    if not opts.is_sentinel:
        _validate_redis_host(opts.host, field_name="Redis host")
        _validate_redis_port(opts.port, field_name="Redis port")
    ssl_ca_certs = getattr(opts, "ssl_ca_certs", None)
    if ssl_ca_certs and not opts.use_ssl:
        raise ValueError("Redis CA configuration requires TLS")

    tls_kwargs = {"ssl": opts.use_ssl}
    if opts.use_ssl:
        tls_kwargs.update({
            "ssl_cert_reqs": "required",
            "ssl_check_hostname": True,
            "ssl_ca_certs": ssl_ca_certs,
        })

    # Decide between standard Redis and Sentinel-based Redis
    if opts.is_sentinel:
        sentinel_hosts = _validated_sentinel_hosts(opts.sentinel_hosts)
        _validate_sentinel_identity(
            opts.sentinel_master,
            field_name="Redis Sentinel master",
        )
        logger.debug(
            f"[redis-catalog] Using Redis Sentinel mode. master={opts.sentinel_master}, "
            f"sentinels={sentinel_hosts}"
        )
        sentinel = Sentinel(
            sentinel_hosts,
            sentinel_kwargs={
                "socket_timeout": 0.5,
                "username": opts.username,
                "password": opts.sentinel_password,
                "decode_responses": decode_responses,
                **tls_kwargs,
            },
            socket_timeout=0.5,
            username=opts.username,
            password=opts.password,
            decode_responses=decode_responses,
            **tls_kwargs,
        )

        sentinel_client = sentinel.master_for(
            opts.sentinel_master,
            db=opts.db,
            username=opts.username,
            password=opts.password,
            decode_responses=decode_responses,
            **tls_kwargs,
        )

        # Fail-fast probe: in some deployments (e.g. standalone Redis without Sentinel),
        # redis-py will only raise on first command. We probe here so we can provide a
        # clearer error (or fall back to direct Redis if configured to do so).
        sentinel_err: Optional[BaseException] = None
        deadline = time.time() + 3.0
        while time.time() < deadline:
            try:
                sentinel_client.ping()
                sentinel_err = None
                break
            except (MasterNotFoundError, redis.RedisError, OSError) as e:
                sentinel_err = e
                time.sleep(0.2)

        if sentinel_err is None:
            return sentinel_client
        logger.error(
            f"[redis-catalog] Sentinel unavailable (strict mode): {sentinel_err}"
        )
        raise sentinel_err

    logger.info(
        f"[redis-catalog] Using standard Redis mode. host={opts.host}, port={opts.port}, db={opts.db}"
    )
    return redis.Redis(
        host=opts.host,
        port=opts.port,
        db=opts.db,
        username=opts.username,
        password=opts.password,
        decode_responses=decode_responses,
        **tls_kwargs,
    )


def close_all_redis_clients() -> None:
    """Close every cached Redis client and disconnect its pool.

    Intended for clean process shutdown only.  After this call any subsequent
    create_redis_client() will rebuild fresh clients on demand.
    """
    with _CLIENT_CACHE_LOCK:
        clients = list(_CLIENT_CACHE.values())
        _CLIENT_CACHE.clear()

    for client in clients:
        try:
            pool = getattr(client, "connection_pool", None)
            if pool is not None:
                pool.disconnect()
        except Exception as e:
            logger.debug(f"[redis-connector] pool disconnect failed: {e}")
        try:
            close = getattr(client, "close", None)
            if callable(close):
                close()
        except Exception as e:
            logger.debug(f"[redis-connector] client close failed: {e}")


class RedisConnector:
    """Returns a process-shared Redis client based on RedisOptions.

    The underlying redis.Redis (and its ConnectionPool) is cached by the
    effective options inside create_redis_client, so repeated construction
    of RedisConnector / RedisCatalog / DataWriter / monitoring loggers does
    not allocate new pools or sockets.
    """

    def __init__(self, options: Optional[RedisOptions] = None):
        self.r = create_redis_client(options)
