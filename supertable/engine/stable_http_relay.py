"""Loopback relay giving rotating bearer URLs one stable DuckDB identity.

DuckDB keys its HTTP metadata and external-file caches by the complete scan
path.  A freshly signed URL therefore looks like a different immutable object
even when only its credential query parameters changed.  This module keeps
credentials out of that identity boundary:

* DuckDB receives an opaque, process-private loopback URL.
* The relay registry maps that URL to the currently admitted bearer URL.
* Provider-issued linked-share cache identities and locally derived identities
  are bound to an immutable snapshot/object seal, never URL normalization.
* Registrations are leased for the complete query/Arrow-stream lifetime.

The relay is deliberately tiny.  It accepts only loopback ``HEAD`` and ``GET``
requests, supports one exact byte range, never follows redirects, and checks
the snapshot-declared object size/range response before forwarding bytes.
Upstream URLs are retained only in the private in-memory registry and are never
returned by diagnostics or written to logs.

For linked shares, the core manifest boundary remains responsible for the
explicit resource-host allowlist and its DNS/origin trust decision.  The relay
does not broaden that decision: it rejects literal/localhost origins, disables
environment proxies, and refuses every redirect, so a signed request cannot be
retargeted after admission.  Deployments should allowlist provider-controlled
DNS names whose resolution policy is appropriate for their network boundary.
"""

from __future__ import annotations

import atexit
import copy
import hashlib
import http.client
import ipaddress
import json
import math
import re
import secrets
import socket
import ssl
import threading
import time
from dataclasses import dataclass, field, replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    HTTPHandler,
    HTTPSHandler,
    ProxyHandler,
    Request,
    build_opener,
)


_CACHE_IDENTITY = re.compile(
    r"(?:share-cache|local-cache)-v1:[0-9a-f]{64}\Z"
)
_ROUTE_PATH = re.compile(r"/v1/([0-9a-f]{64})\Z")
_SINGLE_RANGE = re.compile(r"bytes=(\d*)-(\d*)\Z", re.IGNORECASE)
_CONTENT_RANGE = re.compile(
    r"bytes\s+(\d+)-(\d+)/(\d+)\Z", re.IGNORECASE
)
_STRONG_ETAG_VALUE = re.compile(r"[\x21\x23-\x7e]{1,1024}\Z")
_NUMERIC_HOST_LABEL = re.compile(r"(?:0x[0-9a-f]+|[0-9]+)\Z", re.IGNORECASE)
_MAX_REQUEST_HEADERS = 32
_MAX_REQUEST_HEADER_BYTES = 16 * 1024
_MAX_UPSTREAM_URL_BYTES = 16 * 1024
_RELAY_CHUNK_BYTES = 1024 * 1024
_UPSTREAM_TIMEOUT_SECONDS = 60.0
_MAX_RELAY_CONNECTIONS = 64
_HEADER_READ_TIMEOUT_SECONDS = 2.0
_CLIENT_IO_TIMEOUT_SECONDS = 60.0


_LOCAL_CREDENTIAL_GENERATION_LOCK = threading.Lock()
_LOCAL_CREDENTIAL_GENERATION = 0


class StableHttpRelayError(RuntimeError):
    """A stable relay identity or upstream HTTP contract was invalid."""


def next_local_credential_generation() -> int:
    """Mint a process-local, strictly increasing presign issuance marker.

    Relay registration can happen out of order when concurrent queries finish
    credential refresh at different times.  The generation travels with the
    exact URL returned by that presign call, allowing the registry to reject a
    late older URL without parsing provider-specific signature parameters.
    """
    global _LOCAL_CREDENTIAL_GENERATION
    with _LOCAL_CREDENTIAL_GENERATION_LOCK:
        _LOCAL_CREDENTIAL_GENERATION += 1
        return _LOCAL_CREDENTIAL_GENERATION


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    )


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _seal_payload(seal: object) -> Dict[str, object]:
    if seal is None:
        return {}
    result: Dict[str, object] = {}
    for field_name in (
        "size", "version", "etag", "last_modified_ns", "checksum_sha256",
    ):
        value = getattr(seal, field_name, None)
        if value not in (None, "", 0):
            result[field_name] = value
    return result


def local_resource_cache_identity(
    *,
    organization: str,
    storage: object,
    super_name: str,
    simple_name: str,
    snapshot_version: int,
    raw_key: str,
    size: int,
    object_seal: object = None,
) -> str:
    """Return a process/auth/snapshot-scoped identity for a local presign.

    The storage instance is intentionally part of the authorization scope.
    The persistent DuckDB engine retains that exact instance, so the value is
    stable for every query that can reuse its cache but cannot cross into a
    different principal's storage client.  Snapshot version and an optional
    provider object seal make overwriting a raw key rotate the identity.
    """
    seal_payload = _seal_payload(object_seal)
    # Size and snapshot version are useful bounds but do not prove object
    # immutability. Legacy resources without a provider version/ETag/checksum
    # remain on their per-credential URL and intentionally get no cross-query
    # relay identity.
    if (
        not raw_key
        or "://" in raw_key
        or int(size) <= 0
        or not seal_payload
        or int(seal_payload.get("size", -1)) != int(size)
        # HTTP If-Match is the enforceable condition available to the relay.
        # A version/checksum-only seal must stay on the direct credential path
        # until the signed URL itself is version-specific or bytes are hashed
        # before publication.
        or not seal_payload.get("etag")
    ):
        raise StableHttpRelayError("local resource cache identity is invalid")
    storage_scope = _sha256_text(
        f"{storage.__class__.__module__}.{storage.__class__.__qualname__}"
        f"\0{id(storage)}"
    )
    payload = {
        "v": 1,
        "organization": str(organization or ""),
        "storage_scope": storage_scope,
        "super": str(super_name),
        "table": str(simple_name),
        "snapshot_version": int(snapshot_version),
        "raw_key": str(raw_key),
        "size": int(size),
        "object_seal": seal_payload,
    }
    return f"local-cache-v1:{_sha256_text(_canonical_json(payload))}"


def _validate_identity(value: object) -> str:
    candidate = str(value or "")
    if _CACHE_IDENTITY.fullmatch(candidate) is None:
        raise StableHttpRelayError("stable resource cache identity is invalid")
    return candidate


def _validate_upstream_url(
    value: object,
    *,
    external_share: bool,
) -> str:
    candidate = str(value or "")
    if (
        not candidate
        or len(candidate.encode("utf-8")) > _MAX_UPSTREAM_URL_BYTES
        or "\x00" in candidate
    ):
        raise StableHttpRelayError("stable relay upstream URL is invalid")
    try:
        parsed = urlsplit(candidate)
        parsed.port
    except (TypeError, ValueError) as exc:
        raise StableHttpRelayError(
            "stable relay upstream URL is invalid"
        ) from exc
    if (
        parsed.scheme.casefold() not in {"http", "https"}
        or not parsed.netloc
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.fragment
    ):
        raise StableHttpRelayError("stable relay upstream URL is invalid")
    if external_share:
        hostname = str(parsed.hostname or "").rstrip(".").casefold()
        try:
            ipaddress.ip_address(hostname)
        except ValueError:
            pass
        else:
            # Provider-share URLs have already crossed the consumer's exact
            # resource-host allowlist. Keep that boundary intact here and
            # reject every alternate literal encoding before opening a socket.
            raise StableHttpRelayError(
                "linked stable relay upstream must use an allowlisted hostname"
            )
        labels = hostname.split(".")
        if labels and all(
            _NUMERIC_HOST_LABEL.fullmatch(label) is not None
            for label in labels
        ):
            # socket resolvers accept forms such as 2130706433, 0177.0.0.1,
            # and 0x7f000001 as numeric loopback addresses even though
            # ipaddress intentionally rejects those non-canonical spellings.
            raise StableHttpRelayError(
                "linked stable relay upstream must use an allowlisted hostname"
            )
        if hostname == "localhost" or hostname.endswith(".localhost"):
            raise StableHttpRelayError(
                "linked stable relay upstream must use an allowlisted hostname"
            )
    return candidate


def _normalized_etag(value: object) -> str:
    text = str(value or "").strip()
    if text.startswith("W/"):
        raise StableHttpRelayError("stable relay requires a strong object ETag")
    if len(text) >= 2 and text[0] == text[-1] == '"':
        text = text[1:-1]
    if text and _STRONG_ETAG_VALUE.fullmatch(text) is None:
        raise StableHttpRelayError("stable relay object ETag is invalid")
    return text


@dataclass
class _Route:
    identity: str
    upstream_url: str
    expected_size: int
    expected_etag: str
    credential_expires_ms: Optional[int]
    credential_generation: Optional[int]
    share_publication_generation: Optional[int]
    incarnation: object = field(default_factory=object)
    boundaries: Dict[object, "_LeaseBoundary"] = field(default_factory=dict)
    active_opens: Dict[object, "_ActiveOpen"] = field(default_factory=dict)
    active_transfers: Dict[object, "_ActiveTransfer"] = field(
        default_factory=dict
    )
    leases: int = 0


@dataclass(frozen=True)
class _LeaseBoundary:
    deadline_monotonic: float
    cancel_event: Optional[threading.Event]


@dataclass(frozen=True)
class _RouteView:
    upstream_url: str
    expected_size: int
    expected_etag: str
    deadline_monotonic: float
    incarnation: object


@dataclass
class _ActiveTransfer:
    upstream_response: object
    downstream_socket: socket.socket

    def close(self) -> None:
        try:
            close = getattr(self.upstream_response, "close", None)
            if callable(close):
                close()
        except Exception:
            pass
        try:
            self.downstream_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


class _ActiveOpen:
    """One pre-response connection attempt cancellable across threads."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._connection = None
        self._response = None
        self._closed = False

    def attach_connection(self, connection) -> None:
        with self._lock:
            self._connection = connection
            closed = self._closed
        if closed:
            try:
                self._shutdown_connection(connection)
                connection.close()
            finally:
                raise StableHttpRelayError(
                    "stable relay upstream open was cancelled"
                )

    def attach_response(self, response) -> None:
        with self._lock:
            self._response = response
            closed = self._closed
        if closed:
            try:
                response.close()
            finally:
                raise StableHttpRelayError(
                    "stable relay upstream open was cancelled"
                )

    def check(self) -> None:
        with self._lock:
            closed = self._closed
        if closed:
            raise StableHttpRelayError(
                "stable relay upstream open was cancelled"
            )

    def detach(self) -> None:
        with self._lock:
            self._connection = None
            self._response = None

    def close(self) -> None:
        with self._lock:
            self._closed = True
            response = self._response
            connection = self._connection
            self._response = None
            self._connection = None
        self._shutdown_connection(connection)
        for resource in (response, connection):
            try:
                close = getattr(resource, "close", None)
                if callable(close):
                    close()
            except Exception:
                pass

    @staticmethod
    def _shutdown_connection(connection) -> None:
        sock = getattr(connection, "sock", None)
        try:
            if sock is not None:
                sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


@dataclass
class _RelayMetrics:
    upstream_requests: int = 0
    upstream_bytes: int = 0
    rejected_requests: int = 0


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class _TrackedHTTPConnection(http.client.HTTPConnection):
    def __init__(self, *args, open_attempt: _ActiveOpen, **kwargs) -> None:
        self._open_attempt = open_attempt
        super().__init__(*args, **kwargs)
        open_attempt.attach_connection(self)

    def connect(self) -> None:
        self._open_attempt.check()
        super().connect()
        self._open_attempt.check()


class _TrackedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, *args, open_attempt: _ActiveOpen, **kwargs) -> None:
        self._open_attempt = open_attempt
        super().__init__(*args, **kwargs)
        open_attempt.attach_connection(self)

    def connect(self) -> None:
        self._open_attempt.check()
        # Keep the attempt-visible connection object attached while exposing
        # the raw TCP socket before a potentially slow TLS handshake.
        http.client.HTTPConnection.connect(self)
        self._open_attempt.check()
        server_hostname = self._tunnel_host or self.host
        self.sock = self._context.wrap_socket(
            self.sock,
            server_hostname=server_hostname,
            do_handshake_on_connect=False,
        )
        self._open_attempt.check()
        self.sock.do_handshake()
        self._open_attempt.check()


def _request_open_attempt(request: Request) -> _ActiveOpen:
    attempt = getattr(request, "_supertable_relay_open_attempt", None)
    if not isinstance(attempt, _ActiveOpen):
        raise StableHttpRelayError(
            "stable relay upstream open boundary is unavailable"
        )
    return attempt


class _TrackedHTTPHandler(HTTPHandler):
    def http_open(self, request):
        attempt = _request_open_attempt(request)

        def connection(host, **kwargs):
            return _TrackedHTTPConnection(
                host, open_attempt=attempt, **kwargs,
            )

        response = self.do_open(connection, request)
        attempt.attach_response(response)
        return response


class _TrackedHTTPSHandler(HTTPSHandler):
    def https_open(self, request):
        attempt = _request_open_attempt(request)

        def connection(host, **kwargs):
            return _TrackedHTTPSConnection(
                host, open_attempt=attempt, **kwargs,
            )

        response = self.do_open(
            connection,
            request,
            context=self._context,
            check_hostname=self._check_hostname,
        )
        attempt.attach_response(response)
        return response


class _RelayHeadersTooLarge(OSError):
    """Raw request line/header block crossed the relay's memory bound."""


class _BoundedHeaderReader:
    """Count bytes before ``http.client.parse_headers`` can buffer them."""

    def __init__(self, raw, max_bytes: int) -> None:
        self._raw = raw
        self._remaining = max(0, int(max_bytes))

    def readline(self, size: int = -1) -> bytes:
        if self._remaining <= 0:
            raise _RelayHeadersTooLarge("relay request headers are too large")
        # One sentinel byte proves overflow without ever allowing stdlib's
        # per-line/per-count limits to accumulate megabytes in memory.
        maximum = self._remaining + 1
        if size is not None and int(size) >= 0:
            maximum = min(maximum, int(size))
        line = self._raw.readline(maximum)
        if len(line) > self._remaining:
            self._remaining = 0
            raise _RelayHeadersTooLarge("relay request headers are too large")
        self._remaining -= len(line)
        return line

    def __getattr__(self, name: str):
        return getattr(self._raw, name)


class _RelayHttpServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = False
    request_queue_size = 128

    def __init__(self, server_address, relay: "_StableHttpRelay") -> None:
        self.relay = relay
        self._connection_slots = threading.BoundedSemaphore(
            _MAX_RELAY_CONNECTIONS
        )
        super().__init__(server_address, _RelayHandler)

    def process_request(self, request, client_address) -> None:
        if not self._connection_slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            # Apply before a handler thread exists. A loopback peer that sends
            # only a partial request line/header can occupy at most one bounded
            # slot and is disconnected promptly without route validation.
            request.settimeout(_HEADER_READ_TIMEOUT_SECONDS)
            super().process_request(request, client_address)
        except BaseException:
            self._connection_slots.release()
            self.shutdown_request(request)
            raise

    def process_request_thread(self, request, client_address) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._connection_slots.release()


class _RelayHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "supertable-relay"
    sys_version = ""

    def setup(self) -> None:
        super().setup()
        self._header_deadline_lock = threading.Lock()
        self._header_deadline_token = None
        self._header_deadline_timer = None
        self._header_request_token = None
        self._header_deadline_expired = False

    def log_message(self, format, *args) -> None:
        # Request paths are opaque, but keeping the relay entirely out of
        # application logs avoids turning diagnostics into a route oracle.
        return

    def handle_one_request(self) -> None:
        # Reset for every request on a persistent connection. _relay_request
        # switches to the longer bounded client-I/O timeout only after the
        # complete header block has passed validation.
        token = object()
        timer = threading.Timer(
            _HEADER_READ_TIMEOUT_SECONDS,
            self._expire_header_read,
            args=(token,),
        )
        timer.daemon = True
        with self._header_deadline_lock:
            self._header_deadline_token = token
            self._header_deadline_timer = timer
            self._header_request_token = token
            self._header_deadline_expired = False
        timer.start()
        original_rfile = self.rfile
        self.rfile = _BoundedHeaderReader(
            original_rfile, _MAX_REQUEST_HEADER_BYTES,
        )
        try:
            self.connection.settimeout(_HEADER_READ_TIMEOUT_SECONDS)
            super().handle_one_request()
        except _RelayHeadersTooLarge:
            self.close_connection = True
            try:
                self._empty_response(431)
            except OSError:
                pass
        except (OSError, TimeoutError):
            self.close_connection = True
        finally:
            self.rfile = original_rfile
            self._cancel_header_deadline(token)
            with self._header_deadline_lock:
                if self._header_request_token is token:
                    self._header_request_token = None

    def parse_request(self) -> bool:
        token = self._header_request_token
        try:
            parsed = super().parse_request()
        finally:
            # parse_request returns only after the complete header block has
            # been consumed (or rejected). Stop the absolute slowloris timer;
            # response streaming has its own separately bounded timeout.
            self._cancel_header_deadline(token)
        with self._header_deadline_lock:
            expired = self._header_deadline_expired
        return bool(parsed and not expired)

    def _cancel_header_deadline(self, token) -> None:
        timer = None
        with self._header_deadline_lock:
            if token is not self._header_deadline_token:
                return
            timer = self._header_deadline_timer
            self._header_deadline_token = None
            self._header_deadline_timer = None
        if timer is not None:
            timer.cancel()

    def _expire_header_read(self, token) -> None:
        with self._header_deadline_lock:
            if token is not self._header_deadline_token:
                return
            self._header_deadline_token = None
            self._header_deadline_timer = None
            self._header_deadline_expired = True
        self.close_connection = True
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def do_HEAD(self) -> None:
        self._relay_request(head_only=True)

    def do_GET(self) -> None:
        self._relay_request(head_only=False)

    def do_POST(self) -> None:
        self._empty_response(405, allow=True)

    def do_PUT(self) -> None:
        self._empty_response(405, allow=True)

    def do_DELETE(self) -> None:
        self._empty_response(405, allow=True)

    def do_OPTIONS(self) -> None:
        self._empty_response(405, allow=True)

    def _empty_response(self, status: int, *, allow: bool = False) -> None:
        self.send_response(status)
        if allow:
            self.send_header("Allow", "HEAD, GET")
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True

    def _request_headers_are_bounded(self) -> bool:
        items = list(self.headers.items())
        if len(items) > _MAX_REQUEST_HEADERS:
            return False
        total = sum(
            len(str(name).encode("utf-8"))
            + len(str(value).encode("utf-8"))
            + 4
            for name, value in items
        )
        return total <= _MAX_REQUEST_HEADER_BYTES

    def _relay_request(self, *, head_only: bool) -> None:
        relay = self.server.relay  # type: ignore[attr-defined]
        response_started = False
        if not self._request_headers_are_bounded():
            relay._record_rejection()
            self._empty_response(431)
            return
        if self.headers.get("Transfer-Encoding") or self.headers.get(
            "Content-Length", "0"
        ).strip() not in {"", "0"}:
            relay._record_rejection()
            self._empty_response(400)
            return
        match = _ROUTE_PATH.fullmatch(self.path)
        if match is None:
            relay._record_rejection()
            self._empty_response(404)
            return

        # Header parsing is complete. Keep response backpressure bounded while
        # allowing large DuckDB range reads to proceed at normal throughput.
        try:
            self.connection.settimeout(_CLIENT_IO_TIMEOUT_SECONDS)
        except OSError:
            self.close_connection = True
            return
        route_key = match.group(1)
        route = relay._route(route_key)
        if route is None:
            relay._record_rejection()
            self._empty_response(404)
            return

        requested_range = self.headers.get("Range")
        try:
            normalized_range, expected_start, expected_end = (
                _validated_range(requested_range, route.expected_size)
            )
        except StableHttpRelayError:
            relay._record_rejection()
            self._empty_response(416)
            return

        method = "HEAD" if head_only else "GET"
        headers = {"Accept-Encoding": "identity"}
        if normalized_range is not None:
            headers["Range"] = normalized_range
        if route.expected_etag:
            headers["If-Match"] = f'"{route.expected_etag}"'
        request = Request(route.upstream_url, headers=headers, method=method)
        relay._record_upstream_request()
        transfer_token = None
        open_token = None
        open_attempt = None
        try:
            upstream_timeout = min(
                _UPSTREAM_TIMEOUT_SECONDS,
                relay._remaining_transfer_timeout(
                    route_key, route.incarnation
                ),
            )
            open_token, open_attempt = relay._begin_open(
                route_key, route.incarnation,
            )
            setattr(
                request,
                "_supertable_relay_open_attempt",
                open_attempt,
            )
            response = relay._opener.open(
                request, timeout=upstream_timeout,
            )
            transfer_token = relay._promote_open_to_transfer(
                route_key,
                route.incarnation,
                open_token,
                open_attempt,
                response,
                self.connection,
            )
            open_token = None
            open_attempt = None
            if transfer_token is None:
                self.close_connection = True
                return
            with response:
                relay._remaining_transfer_timeout(
                    route_key, route.incarnation
                )
                if not _headers_are_bounded(response.headers) or not _response_matches_route(
                    response,
                    route,
                    expected_start=expected_start,
                    expected_end=expected_end,
                ):
                    relay._record_rejection()
                    self._empty_response(502)
                    return
                response_length = int(response.headers["Content-Length"])
                response_started = True
                self.send_response(int(response.status))
                self.send_header("Content-Length", str(response_length))
                if normalized_range is not None:
                    self.send_header(
                        "Content-Range", response.headers["Content-Range"],
                    )
                self.send_header("Accept-Ranges", "bytes")
                content_type = response.headers.get("Content-Type")
                if content_type and len(content_type) <= 256:
                    self.send_header("Content-Type", content_type)
                etag = response.headers.get("ETag")
                if etag and len(etag) <= 1024:
                    self.send_header("ETag", etag)
                last_modified = response.headers.get("Last-Modified")
                if last_modified and len(last_modified) <= 256:
                    self.send_header("Last-Modified", last_modified)
                self.end_headers()
                if head_only:
                    return
                remaining = response_length
                bounded_read = getattr(response, "read1", None)
                if not callable(bounded_read):
                    bounded_read = response.read
                while remaining > 0:
                    transfer_remaining = relay._remaining_transfer_timeout(
                        route_key, route.incarnation
                    )
                    try:
                        self.connection.settimeout(min(
                            _CLIENT_IO_TIMEOUT_SECONDS,
                            transfer_remaining,
                        ))
                    except OSError:
                        self.close_connection = True
                        return
                    chunk = bounded_read(
                        min(_RELAY_CHUNK_BYTES, remaining)
                    )
                    relay._remaining_transfer_timeout(
                        route_key, route.incarnation
                    )
                    if not chunk:
                        self.close_connection = True
                        return
                    if len(chunk) > remaining:
                        relay._record_rejection()
                        self.close_connection = True
                        return
                    try:
                        self.wfile.write(chunk)
                    except (BrokenPipeError, ConnectionResetError):
                        self.close_connection = True
                        return
                    remaining -= len(chunk)
                    relay._record_upstream_bytes(len(chunk))
                relay._remaining_transfer_timeout(
                    route_key, route.incarnation
                )
                if bounded_read(1):
                    relay._record_rejection()
                    self.close_connection = True
        except HTTPError as exc:
            # A redirect arrives here because _NoRedirects refuses it. Never
            # forward Location: DuckDB must not leave the admitted origin.
            status = int(getattr(exc, "code", 502) or 502)
            if 300 <= status < 400:
                status = 502
            if response_started:
                self.close_connection = True
            else:
                self._empty_response(status if 400 <= status <= 599 else 502)
        except StableHttpRelayError:
            relay._record_rejection()
            self.close_connection = True
        except Exception:
            if response_started:
                self.close_connection = True
            else:
                try:
                    self._empty_response(502)
                except OSError:
                    self.close_connection = True
        finally:
            if open_token is not None and open_attempt is not None:
                relay._end_open(
                    route_key,
                    route.incarnation,
                    open_token,
                    open_attempt,
                )
            if transfer_token is not None:
                relay._untrack_transfer(route_key, transfer_token)


def _validated_range(
    value: Optional[str], object_size: int,
) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    if value is None:
        return None, None, None
    if len(value) > 128:
        raise StableHttpRelayError("relay range is invalid")
    match = _SINGLE_RANGE.fullmatch(value.strip())
    if match is None or (not match.group(1) and not match.group(2)):
        raise StableHttpRelayError("relay range is invalid")
    if match.group(1):
        start = int(match.group(1))
        end = (
            int(match.group(2))
            if match.group(2)
            else object_size - 1
        )
        if start < 0 or start >= object_size or end < start:
            raise StableHttpRelayError("relay range is invalid")
        end = min(end, object_size - 1)
    else:
        suffix = int(match.group(2))
        if suffix <= 0:
            raise StableHttpRelayError("relay range is invalid")
        start = max(0, object_size - suffix)
        end = object_size - 1
    return f"bytes={start}-{end}", start, end


def _response_matches_route(
    response,
    route: _Route,
    *,
    expected_start: Optional[int],
    expected_end: Optional[int],
) -> bool:
    try:
        status = int(response.status)
        content_length = int(response.headers.get("Content-Length", ""))
    except (TypeError, ValueError):
        return False
    if content_length < 0:
        return False
    encoding = str(response.headers.get("Content-Encoding", "") or "")
    if encoding.casefold() not in {"", "identity"}:
        return False
    if response.headers.get("Transfer-Encoding"):
        return False
    try:
        upstream_etag = _normalized_etag(response.headers.get("ETag"))
    except StableHttpRelayError:
        return False
    if not upstream_etag or upstream_etag != route.expected_etag:
        return False
    if expected_start is None or expected_end is None:
        return status == 200 and content_length == route.expected_size
    if status != 206 or content_length != expected_end - expected_start + 1:
        return False
    content_range = str(response.headers.get("Content-Range", "") or "")
    match = _CONTENT_RANGE.fullmatch(content_range.strip())
    if match is None:
        return False
    return (
        int(match.group(1)) == expected_start
        and int(match.group(2)) == expected_end
        and int(match.group(3)) == route.expected_size
    )


def _headers_are_bounded(headers) -> bool:
    try:
        items = list(headers.items())
        if len(items) > _MAX_REQUEST_HEADERS:
            return False
        total = sum(
            len(str(name).encode("utf-8"))
            + len(str(value).encode("utf-8"))
            + 4
            for name, value in items
        )
    except Exception:
        return False
    return total <= _MAX_REQUEST_HEADER_BYTES


class _RouteLease:
    def __init__(
        self,
        relay: "_StableHttpRelay",
        route_key: str,
        lease_token: object,
        url: str,
    ):
        self._relay = relay
        self._route_key = route_key
        self._lease_token = lease_token
        self.url = url
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._relay._release(self._route_key, self._lease_token)


class StableRelayLease:
    """One idempotent lease over every route used by a reflection."""

    def __init__(self, leases: Iterable[_RouteLease] = ()) -> None:
        self._leases = list(leases)
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for lease in reversed(self._leases):
            lease.close()


class _StableHttpRelay:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._route_condition = threading.Condition(self._lock)
        self._process_secret = secrets.token_bytes(32)
        self._routes: Dict[str, _Route] = {}
        self._active_open_count = 0
        self._metrics = _RelayMetrics()
        self._closed = False
        self._opener = build_opener(
            ProxyHandler({}),
            _NoRedirects(),
            _TrackedHTTPHandler(),
            _TrackedHTTPSHandler(context=ssl.create_default_context()),
        )
        try:
            self._server = _RelayHttpServer(("127.0.0.1", 0), self)
        except OSError as exc:
            raise StableHttpRelayError(
                "stable loopback relay is unavailable"
            ) from exc
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.1},
            name="supertable-stable-http-relay",
            daemon=True,
        )
        self._thread.start()
        self._watchdog_thread = threading.Thread(
            target=self._watch_route_boundaries,
            name="supertable-stable-http-relay-watchdog",
            daemon=True,
        )
        self._watchdog_thread.start()

    def register(
        self,
        identity: str,
        upstream_url: str,
        *,
        expected_size: int,
        expected_etag: str = "",
        credential_expires_ms: Optional[int] = None,
        credential_generation: Optional[int] = None,
        share_publication_generation: Optional[int] = None,
        deadline_monotonic: Optional[float] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> _RouteLease:
        identity = _validate_identity(identity)
        external_share = identity.startswith("share-cache-v1:")
        upstream_url = _validate_upstream_url(
            upstream_url,
            external_share=external_share,
        )
        try:
            expected_size = int(expected_size)
        except (TypeError, ValueError, OverflowError) as exc:
            raise StableHttpRelayError(
                "stable relay object size is invalid"
            ) from exc
        if expected_size <= 0:
            raise StableHttpRelayError("stable relay object size is invalid")
        expected_etag = _normalized_etag(expected_etag)
        if not expected_etag or len(expected_etag.encode("utf-8")) > 1024:
            raise StableHttpRelayError(
                "stable relay requires an enforceable object ETag"
            )
        if credential_expires_ms is not None:
            if (
                not isinstance(credential_expires_ms, int)
                or isinstance(credential_expires_ms, bool)
                or credential_expires_ms <= 0
            ):
                raise StableHttpRelayError(
                    "stable relay credential expiry is invalid"
                )
        elif external_share:
            raise StableHttpRelayError(
                "linked stable relay credential expiry is unavailable"
            )
        if credential_generation is not None:
            if (
                not isinstance(credential_generation, int)
                or isinstance(credential_generation, bool)
                or credential_generation <= 0
            ):
                raise StableHttpRelayError(
                    "stable relay credential generation is invalid"
                )
        if share_publication_generation is not None and (
            not isinstance(share_publication_generation, int)
            or isinstance(share_publication_generation, bool)
            or share_publication_generation <= 0
        ):
            raise StableHttpRelayError(
                "linked stable relay publication generation is invalid"
            )
        if external_share:
            if credential_generation is not None:
                raise StableHttpRelayError(
                    "linked stable relay credential generation is invalid"
                )
        else:
            if share_publication_generation is not None:
                raise StableHttpRelayError(
                    "local stable relay publication generation is invalid"
                )
            if credential_expires_ms is None:
                raise StableHttpRelayError(
                    "local stable relay credential expiry is unavailable"
                )
            if credential_generation is None:
                raise StableHttpRelayError(
                    "local stable relay credential generation is unavailable"
                )
        if deadline_monotonic is None:
            deadline_monotonic = time.monotonic() + _UPSTREAM_TIMEOUT_SECONDS
        try:
            deadline_monotonic = float(deadline_monotonic)
        except (TypeError, ValueError, OverflowError) as exc:
            raise StableHttpRelayError(
                "stable relay transfer deadline is invalid"
            ) from exc
        if (
            not math.isfinite(deadline_monotonic)
            or deadline_monotonic <= time.monotonic()
        ):
            raise StableHttpRelayError(
                "stable relay transfer deadline is invalid"
            )
        if cancel_event is not None:
            is_set = getattr(cancel_event, "is_set", None)
            if not callable(is_set):
                raise StableHttpRelayError(
                    "stable relay cancellation boundary is invalid"
                )
            if is_set():
                raise StableHttpRelayError(
                    "stable relay transfer was cancelled"
                )
        route_key = hashlib.sha256(
            self._process_secret + b"\0" + identity.encode("ascii")
        ).hexdigest()
        lease_token = object()
        boundary = _LeaseBoundary(
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        )
        retired: List[object] = []
        try:
            with self._route_condition:
                if self._closed:
                    raise StableHttpRelayError(
                        "stable loopback relay is closed"
                    )
                if cancel_event is not None and cancel_event.is_set():
                    raise StableHttpRelayError(
                        "stable relay transfer was cancelled"
                    )
                existing = self._routes.get(route_key)
                if existing is not None and self._prune_inactive_boundaries_locked(
                    existing, time.monotonic()
                ):
                    retired.extend(self._retire_route_locked(
                        route_key, existing
                    ))
                    existing = None
                if existing is not None and (
                    existing.identity != identity
                    or existing.expected_size != expected_size
                    or existing.expected_etag != expected_etag
                ):
                    raise StableHttpRelayError(
                        "stable relay identity has conflicting immutable metadata"
                    )
                if existing is None:
                    existing = _Route(
                        identity=identity,
                        upstream_url=upstream_url,
                        expected_size=expected_size,
                        expected_etag=expected_etag,
                        credential_expires_ms=credential_expires_ms,
                        credential_generation=credential_generation,
                        share_publication_generation=(
                            share_publication_generation
                        ),
                    )
                    self._routes[route_key] = existing
                else:
                    # While any query leases this stable URL, never trade a
                    # longer-valid credential for a shorter one. Equal-expiry
                    # ties use the authority's monotonic issuance/publication
                    # order; equal metadata with different URLs is ambiguous.
                    assert credential_expires_ms is not None
                    assert existing.credential_expires_ms is not None
                    should_refresh = False
                    if credential_expires_ms > existing.credential_expires_ms:
                        should_refresh = True
                    elif credential_expires_ms == existing.credential_expires_ms:
                        if external_share:
                            new_order = share_publication_generation
                            old_order = existing.share_publication_generation
                            ambiguity = "linked publication generation is ambiguous"
                        else:
                            new_order = credential_generation
                            old_order = existing.credential_generation
                            ambiguity = "local credential generation is ambiguous"
                        if new_order is not None and old_order is not None:
                            should_refresh = new_order > old_order
                            if (
                                new_order == old_order
                                and upstream_url != existing.upstream_url
                            ):
                                raise StableHttpRelayError(ambiguity)
                        elif upstream_url != existing.upstream_url:
                            raise StableHttpRelayError(ambiguity)
                    if should_refresh:
                        existing.upstream_url = upstream_url
                        existing.credential_expires_ms = credential_expires_ms
                        existing.credential_generation = credential_generation
                        existing.share_publication_generation = (
                            share_publication_generation
                        )
                existing.boundaries[lease_token] = boundary
                existing.leases = len(existing.boundaries)
                self._route_condition.notify_all()
                port = int(self._server.server_address[1])
                relay_url = f"http://127.0.0.1:{port}/v1/{route_key}"
        finally:
            self._close_transfers(retired)
        return _RouteLease(self, route_key, lease_token, relay_url)

    @staticmethod
    def _prune_inactive_boundaries_locked(route: _Route, now: float) -> bool:
        inactive = [
            token
            for token, boundary in route.boundaries.items()
            if boundary.deadline_monotonic <= now
            or (
                boundary.cancel_event is not None
                and boundary.cancel_event.is_set()
            )
        ]
        for token in inactive:
            route.boundaries.pop(token, None)
        route.leases = len(route.boundaries)
        return not route.boundaries

    def _retire_route_locked(
        self,
        route_key: str,
        route: _Route,
    ) -> List[object]:
        if self._routes.get(route_key) is route:
            self._routes.pop(route_key, None)
        route.boundaries.clear()
        route.leases = 0
        opens = list(route.active_opens.values())
        self._active_open_count = max(
            0, self._active_open_count - len(opens)
        )
        route.active_opens.clear()
        transfers: List[object] = opens
        transfers.extend(route.active_transfers.values())
        route.active_transfers.clear()
        return transfers

    @staticmethod
    def _close_transfers(transfers: Iterable[object]) -> None:
        for transfer in transfers:
            close = getattr(transfer, "close", None)
            if callable(close):
                close()

    def _route(self, route_key: str) -> Optional[_RouteView]:
        retired: List[object] = []
        view = None
        with self._route_condition:
            route = self._routes.get(route_key)
            if route is not None and self._prune_inactive_boundaries_locked(
                route, time.monotonic()
            ):
                retired.extend(self._retire_route_locked(route_key, route))
                self._route_condition.notify_all()
                route = None
            if route is not None:
                view = _RouteView(
                    upstream_url=route.upstream_url,
                    expected_size=route.expected_size,
                    expected_etag=route.expected_etag,
                    deadline_monotonic=max(
                        item.deadline_monotonic
                        for item in route.boundaries.values()
                    ),
                    incarnation=route.incarnation,
                )
        self._close_transfers(retired)
        return view

    def _remaining_transfer_timeout(
        self,
        route_key: str,
        incarnation: object,
    ) -> float:
        retired: List[object] = []
        remaining = 0.0
        with self._route_condition:
            route = self._routes.get(route_key)
            now = time.monotonic()
            if (
                route is None
                or route.incarnation is not incarnation
                or self._prune_inactive_boundaries_locked(route, now)
            ):
                if route is not None:
                    if route.incarnation is incarnation:
                        retired.extend(self._retire_route_locked(
                            route_key, route
                        ))
                        self._route_condition.notify_all()
            else:
                remaining = max(
                    item.deadline_monotonic
                    for item in route.boundaries.values()
                ) - now
        self._close_transfers(retired)
        if remaining <= 0:
            raise StableHttpRelayError(
                "stable relay transfer boundary expired"
            )
        return remaining

    def _begin_open(
        self,
        route_key: str,
        incarnation: object,
    ) -> Tuple[object, _ActiveOpen]:
        retired: List[object] = []
        open_token = None
        attempt = None
        with self._route_condition:
            route = self._routes.get(route_key)
            if (
                route is not None
                and route.incarnation is incarnation
                and self._prune_inactive_boundaries_locked(
                    route, time.monotonic()
                )
            ):
                retired.extend(self._retire_route_locked(route_key, route))
                self._route_condition.notify_all()
                route = None
            if route is not None and route.incarnation is incarnation:
                if self._active_open_count >= _MAX_RELAY_CONNECTIONS:
                    raise StableHttpRelayError(
                        "stable relay upstream open capacity is exhausted"
                    )
                open_token = object()
                attempt = _ActiveOpen()
                route.active_opens[open_token] = attempt
                self._active_open_count += 1
        self._close_transfers(retired)
        if open_token is None or attempt is None:
            raise StableHttpRelayError(
                "stable relay transfer boundary expired"
            )
        return open_token, attempt

    def _promote_open_to_transfer(
        self,
        route_key: str,
        incarnation: object,
        open_token: object,
        attempt: _ActiveOpen,
        upstream_response: object,
        downstream_socket: socket.socket,
    ) -> Optional[object]:
        retired: List[object] = []
        transfer_token = None
        with self._route_condition:
            route = self._routes.get(route_key)
            registered = (
                route is not None
                and route.incarnation is incarnation
                and route.active_opens.get(open_token) is attempt
            )
            if registered:
                route.active_opens.pop(open_token, None)
                self._active_open_count = max(
                    0, self._active_open_count - 1
                )
                if self._prune_inactive_boundaries_locked(
                    route, time.monotonic()
                ):
                    retired.append(attempt)
                    retired.extend(self._retire_route_locked(
                        route_key, route
                    ))
                    self._route_condition.notify_all()
                else:
                    attempt.detach()
                    transfer_token = object()
                    route.active_transfers[transfer_token] = _ActiveTransfer(
                        upstream_response=upstream_response,
                        downstream_socket=downstream_socket,
                    )
            else:
                retired.append(attempt)
        self._close_transfers(retired)
        return transfer_token

    def _end_open(
        self,
        route_key: str,
        incarnation: object,
        open_token: object,
        attempt: _ActiveOpen,
    ) -> None:
        with self._route_condition:
            route = self._routes.get(route_key)
            if (
                route is not None
                and route.incarnation is incarnation
                and route.active_opens.get(open_token) is attempt
            ):
                route.active_opens.pop(open_token, None)
                self._active_open_count = max(
                    0, self._active_open_count - 1
                )
                self._route_condition.notify_all()
        attempt.close()

    def _untrack_transfer(self, route_key: str, transfer_token: object) -> None:
        with self._route_condition:
            route = self._routes.get(route_key)
            if route is not None:
                route.active_transfers.pop(transfer_token, None)

    def _release(self, route_key: str, lease_token: object) -> None:
        retired: List[object] = []
        with self._route_condition:
            route = self._routes.get(route_key)
            if route is None:
                return
            route.boundaries.pop(lease_token, None)
            route.leases = len(route.boundaries)
            if not route.boundaries:
                # DuckDB retains the opaque path as a cache identity, but no
                # bearer URL or in-flight response survives its final query.
                retired.extend(self._retire_route_locked(route_key, route))
            self._route_condition.notify_all()
        self._close_transfers(retired)

    def _watch_route_boundaries(self) -> None:
        while True:
            retired: List[object] = []
            with self._route_condition:
                if self._closed:
                    return
                now = time.monotonic()
                for route_key, route in list(self._routes.items()):
                    if self._prune_inactive_boundaries_locked(route, now):
                        retired.extend(self._retire_route_locked(
                            route_key, route
                        ))
                if not retired:
                    # Cancellation Events cannot notify this Condition, so a
                    # short bounded poll complements exact deadline wakeups.
                    nearest = min(
                        (
                            boundary.deadline_monotonic
                            for route in self._routes.values()
                            for boundary in route.boundaries.values()
                        ),
                        default=now + 0.01,
                    )
                    self._route_condition.wait(timeout=max(
                        0.001, min(0.01, nearest - now)
                    ))
            self._close_transfers(retired)

    def _record_upstream_request(self) -> None:
        with self._lock:
            self._metrics.upstream_requests += 1

    def _record_upstream_bytes(self, size: int) -> None:
        with self._lock:
            self._metrics.upstream_bytes += max(0, int(size))

    def _record_rejection(self) -> None:
        with self._lock:
            self._metrics.rejected_requests += 1

    def metrics(self) -> Dict[str, int]:
        with self._lock:
            return {
                "active_routes": len(self._routes),
                "active_upstream_opens": self._active_open_count,
                "upstream_requests": self._metrics.upstream_requests,
                "upstream_bytes": self._metrics.upstream_bytes,
                "rejected_requests": self._metrics.rejected_requests,
            }

    def reset_metrics(self) -> None:
        with self._lock:
            self._metrics = _RelayMetrics()

    def close(self) -> None:
        retired: List[object] = []
        with self._route_condition:
            if self._closed:
                return
            self._closed = True
            for route_key, route in list(self._routes.items()):
                retired.extend(self._retire_route_locked(route_key, route))
            self._route_condition.notify_all()
        self._close_transfers(retired)
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not threading.current_thread():
            self._thread.join(timeout=1.0)
        if self._watchdog_thread is not threading.current_thread():
            self._watchdog_thread.join(timeout=1.0)


_RELAY_LOCK = threading.Lock()
_RELAY: Optional[_StableHttpRelay] = None


def _get_relay() -> _StableHttpRelay:
    global _RELAY
    with _RELAY_LOCK:
        if _RELAY is None:
            _RELAY = _StableHttpRelay()
        return _RELAY


def stable_http_relay_metrics(*, reset: bool = False) -> Dict[str, int]:
    """Return data-free counters; never includes routes, identities, or URLs."""
    relay = _get_relay()
    metrics = relay.metrics()
    if reset:
        relay.reset_metrics()
    return metrics


def shutdown_stable_http_relay() -> None:
    """Stop the process relay and erase all in-memory bearer registrations."""
    global _RELAY
    with _RELAY_LOCK:
        relay = _RELAY
        _RELAY = None
    if relay is not None:
        relay.close()


def _path_is_bearer_http_url(value: object) -> bool:
    try:
        parsed = urlsplit(str(value or ""))
    except Exception:
        return False
    return (
        parsed.scheme.casefold() in {"http", "https"}
        and bool(parsed.query)
    )


def alias_stable_remote_paths(
    reflection,
    *,
    storage: object,
    organization: str,
    deadline_monotonic: Optional[float] = None,
    cancel_event: Optional[threading.Event] = None,
) -> Tuple[Any, StableRelayLease]:
    """Clone a reflection and replace rotating bearer paths with stable URLs.

    Linked snapshots must carry provider-issued identities one-for-one.  Local
    presigns derive an authorization/snapshot-scoped identity from their raw
    catalog key.  Ordinary local, S3, and unsigned HTTPS paths are unchanged.
    """
    route_leases: List[_RouteLease] = []
    supers = []
    changed = False
    relay = None
    try:
        for snapshot in list(getattr(reflection, "supers", ()) or ()):
            files = list(getattr(snapshot, "files", ()) or ())
            raw_keys = list(getattr(snapshot, "resource_keys", ()) or ())
            sizes = list(getattr(snapshot, "resource_sizes", ()) or ())
            supplied_identities = list(
                getattr(snapshot, "resource_cache_identities", ()) or ()
            )
            credential_generations = list(
                getattr(snapshot, "resource_credential_generations", ()) or ()
            )
            local_credential_expiries = list(
                getattr(snapshot, "resource_credential_expires_ms", ()) or ()
            )
            is_linked = bool(
                getattr(snapshot, "share_policy_fingerprint", None)
            )
            credential_expires_ms = getattr(
                snapshot, "share_credential_expires_ms", None,
            )
            share_publication_generation = getattr(
                snapshot, "share_publication_generation", None,
            )
            if (
                is_linked
                and supplied_identities
                and len(supplied_identities) != len(files)
            ):
                raise StableHttpRelayError(
                    "stable resource identities do not match the reflection"
                )
            if is_linked and any(
                _path_is_bearer_http_url(path) for path in files
            ) and len(supplied_identities) != len(files):
                raise StableHttpRelayError(
                    "linked reflection has no stable resource identities"
                )
            if len(files) != len(raw_keys) or (sizes and len(sizes) != len(files)):
                if is_linked and any(
                    _path_is_bearer_http_url(path) for path in files
                ):
                    raise StableHttpRelayError(
                        "linked reflection resource metadata is incomplete"
                    )
                supers.append(snapshot)
                continue

            aliased_files = list(files)
            seals = getattr(snapshot, "resource_object_seals", None) or {}
            for index, (path, raw_key) in enumerate(zip(files, raw_keys)):
                if not _path_is_bearer_http_url(path):
                    continue
                try:
                    size = int(sizes[index]) if sizes else 0
                except (TypeError, ValueError, OverflowError):
                    size = 0
                if size <= 0:
                    if is_linked:
                        raise StableHttpRelayError(
                            "linked reflection object size is invalid"
                        )
                    continue

                seal = seals.get(str(raw_key))
                expected_etag = getattr(seal, "etag", "") if seal else ""
                # Stable cache reuse is admitted only when the relay can send
                # If-Match and verify the response ETag. Provider identities
                # scoped by a per-manifest nonce remain direct when no such
                # immutable condition exists.
                if not expected_etag:
                    continue

                if is_linked:
                    cache_identity = _validate_identity(
                        supplied_identities[index]
                    )
                else:
                    if (
                        len(credential_generations) != len(files)
                        or not isinstance(credential_generations[index], int)
                        or isinstance(credential_generations[index], bool)
                        or credential_generations[index] <= 0
                        or len(local_credential_expiries) != len(files)
                        or not isinstance(
                            local_credential_expiries[index], int
                        )
                        or isinstance(local_credential_expiries[index], bool)
                        or local_credential_expiries[index] <= 0
                    ):
                        # A stable identity without issuance ordering would let
                        # a late concurrent query restore an older local bearer
                        # URL. Keep legacy reflections on their direct path.
                        continue
                    if not raw_key or "://" in str(raw_key):
                        # A URL without a provider-issued identity must never be
                        # normalized into an apparent stable object key.
                        if is_linked:
                            raise StableHttpRelayError(
                                "linked reflection cache identity is invalid"
                            )
                        continue
                    try:
                        cache_identity = local_resource_cache_identity(
                            organization=organization,
                            storage=storage,
                            super_name=getattr(snapshot, "super_name", ""),
                            simple_name=getattr(snapshot, "simple_name", ""),
                            snapshot_version=getattr(
                                snapshot, "simple_version", 0,
                            ),
                            raw_key=str(raw_key),
                            size=size,
                            object_seal=seals.get(str(raw_key)),
                        )
                    except StableHttpRelayError:
                        # A legacy local resource with size-only metadata has no
                        # safe cross-query identity. Keep its original signed
                        # URL so an overwrite cannot inherit cached bytes.
                        continue
                if relay is None:
                    relay = _get_relay()
                route_lease = relay.register(
                    cache_identity,
                    str(path),
                    expected_size=size,
                    expected_etag=expected_etag,
                    credential_expires_ms=(
                        credential_expires_ms
                        if is_linked
                        else local_credential_expiries[index]
                    ),
                    credential_generation=(
                        None if is_linked else credential_generations[index]
                    ),
                    share_publication_generation=(
                        share_publication_generation if is_linked else None
                    ),
                    deadline_monotonic=deadline_monotonic,
                    cancel_event=cancel_event,
                )
                route_leases.append(route_lease)
                aliased_files[index] = route_lease.url

            if aliased_files != files:
                changed = True
                try:
                    snapshot = replace(snapshot, files=aliased_files)
                except TypeError:
                    snapshot = copy.copy(snapshot)
                    snapshot.files = aliased_files
            supers.append(snapshot)

        if not changed:
            return reflection, StableRelayLease(route_leases)
        try:
            cloned = replace(reflection, supers=supers)
        except TypeError:
            cloned = copy.copy(reflection)
            cloned.supers = supers
        return cloned, StableRelayLease(route_leases)
    except BaseException:
        StableRelayLease(route_leases).close()
        raise


atexit.register(shutdown_stable_http_relay)
