from __future__ import annotations

import io
import select
import socket
import threading
import time
from dataclasses import replace as dataclass_replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import supertable.engine.stable_http_relay as relay_module
import supertable.engine.executor as executor_module
import supertable.engine.engine_common as engine_common
from supertable.data_classes import (
    Reflection,
    ResourceObjectSeal,
    SuperSnapshot,
)
from supertable.engine.stable_http_relay import (
    StableHttpRelayError,
    _get_relay,
    alias_stable_remote_paths,
    local_resource_cache_identity,
    shutdown_stable_http_relay,
    stable_http_relay_metrics,
)
from supertable.engine.duckdb_engine import DuckDB
from supertable.engine.executor import _refresh_presigned_reflection
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser


def _assert_http_error_code(caught, expected: int) -> None:
    """Assert an HTTP failure without retaining its response socket."""

    error = caught.value
    try:
        assert error.code == expected
    finally:
        error.close()


def _record_thread_error(outcome: dict, error: BaseException) -> None:
    """Retain a worker failure without retaining an HTTP response socket."""

    try:
        outcome["error"] = error
    finally:
        if isinstance(error, HTTPError):
            error.close()


def _parquet_bytes() -> bytes:
    sink = io.BytesIO()
    pq.write_table(
        pa.table({
            "id": list(range(1_000)),
            "value": [f"value-{index:04d}" for index in range(1_000)],
            "__rowid__": list(range(1_000)),
            "__timestamp__": [1] * 1_000,
        }),
        sink,
        row_group_size=100,
    )
    return sink.getvalue()


class _Origin:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.etag = "etag-v1"
        self.requests = []
        self.bytes_sent = 0
        self._lock = threading.Lock()
        self.slow_drip_started = threading.Event()
        self.slow_headers_started = threading.Event()

        origin = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format, *args):
                return

            def do_HEAD(self):
                self._serve(head_only=True)

            def do_GET(self):
                self._serve(head_only=False)

            def _serve(self, *, head_only: bool):
                with origin._lock:
                    origin.requests.append({
                        "method": self.command,
                        "path": self.path,
                        "range": self.headers.get("Range"),
                        "if_match": self.headers.get("If-Match"),
                    })

                if self.path.startswith("/redirect"):
                    self.send_response(302)
                    self.send_header(
                        "Location",
                        f"http://127.0.0.1:{self.server.server_port}/data",
                    )
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return

                if_match = self.headers.get("If-Match")
                if if_match and if_match != f'"{origin.etag}"':
                    self.send_response(412)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return

                requested_range = self.headers.get("Range")
                ignore_range = "bad-range=1" in self.path
                if requested_range and not ignore_range:
                    raw_start, raw_end = requested_range[6:].split("-", 1)
                    start = int(raw_start)
                    end = int(raw_end) if raw_end else len(origin.payload) - 1
                    end = min(end, len(origin.payload) - 1)
                    body = origin.payload[start:end + 1]
                    status = 206
                    content_range = (
                        f"bytes {start}-{end}/{len(origin.payload)}"
                    )
                else:
                    body = origin.payload
                    status = 200
                    content_range = None

                if "slow-headers=1" in self.path:
                    lines = [
                        f"HTTP/1.1 {status} OK",
                        f"Content-Length: {len(body)}",
                        "Accept-Ranges: bytes",
                        f'ETag: "{origin.etag}"',
                        "Content-Type: application/octet-stream",
                    ]
                    if content_range is not None:
                        lines.append(f"Content-Range: {content_range}")
                    raw_headers = ("\r\n".join(lines) + "\r\n\r\n").encode(
                        "ascii"
                    )
                    origin.slow_headers_started.set()
                    for value in raw_headers:
                        try:
                            self.connection.sendall(bytes([value]))
                        except OSError:
                            return
                        time.sleep(0.02)
                    return

                self.send_response(status)
                if content_range is not None:
                    self.send_header("Content-Range", content_range)

                self.send_header("Content-Length", str(len(body)))
                self.send_header("Accept-Ranges", "bytes")
                if "no-etag=1" not in self.path:
                    self.send_header("ETag", f'"{origin.etag}"')
                self.send_header("Content-Type", "application/octet-stream")
                if "many-headers=1" in self.path:
                    for index in range(40):
                        self.send_header(f"X-Test-{index}", "bounded")
                self.end_headers()
                if not head_only:
                    if "slow-drip=1" in self.path:
                        origin.slow_drip_started.set()
                        for value in body:
                            try:
                                self.wfile.write(bytes([value]))
                                self.wfile.flush()
                            except (BrokenPipeError, ConnectionResetError):
                                return
                            with origin._lock:
                                origin.bytes_sent += 1
                            time.sleep(0.02)
                        return
                    try:
                        self.wfile.write(body)
                    except (BrokenPipeError, ConnectionResetError):
                        return
                    with origin._lock:
                        origin.bytes_sent += len(body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            name="stable-relay-test-origin",
            daemon=True,
        )

    def start(self) -> "_Origin":
        self.thread.start()
        return self

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=1.0)


@pytest.fixture(autouse=True)
def _isolated_relay():
    shutdown_stable_http_relay()
    yield
    shutdown_stable_http_relay()


@pytest.fixture
def origin():
    value = _Origin(_parquet_bytes()).start()
    try:
        yield value
    finally:
        value.close()


def _linked_reflection(
    url: str,
    *,
    cache_identity: str,
    size: int,
    expires_ms: int,
    publication_generation: int = 1,
    etag: str = "etag-v1",
    policy_fingerprint: str = "f" * 64,
    row_filter: str | None = None,
) -> Reflection:
    return Reflection(
        storage_type="LinkedShare",
        reflection_bytes=size,
        total_reflections=1,
        supers=[SuperSnapshot(
            super_name="shared",
            simple_name="events",
            simple_version=7,
            files=[url],
            # Linked virtual leaves intentionally carry the provider URL here;
            # only the provider-issued opaque cache identity is stable.
            resource_keys=[url],
            resource_sizes=[size],
            resource_object_seals={
                url: ResourceObjectSeal(
                    size=size, version="provider-version-1", etag=etag,
                ),
            },
            columns={"id", "value"},
            share_policy_fingerprint=policy_fingerprint,
            share_row_filter=row_filter,
            # This represents an unrestricted share: every schema column is
            # authorized and there is no row predicate.
            share_allowed_columns=["id", "value"],
            share_credential_expires_ms=expires_ms,
            share_publication_generation=publication_generation,
            resource_cache_identities=[cache_identity],
        )],
    )


def _local_reflection(
    url: str,
    *,
    raw_key: str,
    size: int,
    seal: ResourceObjectSeal | None,
    version: int = 7,
    credential_generation: int | None = 1,
    credential_expires_ms: int | None = None,
) -> Reflection:
    if credential_expires_ms is None:
        credential_expires_ms = int(time.time() * 1_000) + 60_000
    return Reflection(
        storage_type="FakeRemote",
        reflection_bytes=size,
        total_reflections=1,
        supers=[SuperSnapshot(
            super_name="lake",
            simple_name="events",
            simple_version=version,
            files=[url],
            resource_keys=[raw_key],
            resource_sizes=[size],
            resource_object_seals=({raw_key: seal} if seal else {}),
            # DataEstimator represents local resources with aligned None
            # placeholders; these must not suppress local identity derivation.
            resource_cache_identities=[None],
            resource_credential_generations=[credential_generation],
            resource_credential_expires_ms=[credential_expires_ms],
            columns={"id", "value"},
        )],
    )


def _map_test_hostname(monkeypatch, hostname: str) -> None:
    original = socket.getaddrinfo

    def resolve(host, *args, **kwargs):
        if str(host).rstrip(".").casefold() == hostname.casefold():
            host = "127.0.0.1"
        return original(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    monkeypatch.setattr(
        relay_module,
        "_is_public_upstream_address",
        lambda address: address.is_global or address.is_loopback,
    )
    monkeypatch.setattr(
        relay_module, "_is_secure_external_share_url", lambda _parsed: True,
    )


def _relay_object_key(url: str) -> str:
    parts = urlsplit(url).path.split("/")
    assert len(parts) == 4 and parts[1] == "v1"
    assert len(parts[2]) == 64 and len(parts[3]) == 64
    return parts[2]


def test_external_share_rejects_plaintext_http() -> None:
    with pytest.raises(StableHttpRelayError, match="HTTPS"):
        relay_module._validate_upstream_url(
            "http://objects.example/data?sig=secret",
            external_share=True,
        )


def test_linked_share_uses_stable_object_key_but_isolated_lease_urls(
    origin, monkeypatch,
):
    hostname = "objects.relay-test.invalid"
    _map_test_hostname(monkeypatch, hostname)
    identity = "share-cache-v1:" + "a" * 64
    expires_ms = int(time.time() * 1_000) + 60_000
    first = _linked_reflection(
        f"http://{hostname}:{origin.server.server_port}/data?signature=one",
        cache_identity=identity,
        size=len(origin.payload),
        expires_ms=expires_ms,
    )
    first_alias, first_lease = alias_stable_remote_paths(
        first, storage=object(), organization="consumer-org",
    )
    stable_path = first_alias.supers[0].files[0]
    assert stable_path.startswith("http://127.0.0.1:")
    assert "signature" not in stable_path

    con = duckdb.connect()
    try:
        con.execute("SET enable_external_file_cache=true")
        con.execute("SET enable_http_metadata_cache=true")
        assert con.execute(
            "SELECT count(*), sum(id) FROM read_parquet(?)", [stable_path],
        ).fetchone() == (1_000, 499_500)
        first_bytes = origin.bytes_sent
        first_request_count = len(origin.requests)
        assert first_bytes > 0
        assert all("signature=one" in item["path"] for item in origin.requests)
    finally:
        first_lease.close()

    second = _linked_reflection(
        f"http://{hostname}:{origin.server.server_port}/data?signature=two",
        cache_identity=identity,
        size=len(origin.payload),
        expires_ms=expires_ms + 1_000,
    )
    second_alias, second_lease = alias_stable_remote_paths(
        second, storage=object(), organization="consumer-org",
    )
    try:
        second_path = second_alias.supers[0].files[0]
        assert second_path != stable_path
        assert _relay_object_key(second_path) == _relay_object_key(stable_path)
        # A completed lease URL is not reactivated under the next query's
        # authority, even while that next lease is live.
        with pytest.raises(HTTPError) as retired:
            urlopen(Request(stable_path, method="HEAD"), timeout=1.0)
        _assert_http_error_code(retired, 404)
        assert con.execute(
            "SELECT count(*), sum(id) FROM read_parquet(?)",
            [second_path],
        ).fetchone() == (1_000, 499_500)
        # Per-query URL isolation intentionally trades cross-query DuckDB cache
        # reuse for cancellation/deadline isolation.
        assert origin.bytes_sent > first_bytes
        assert len(origin.requests) > first_request_count
        assert any(
            "signature=two" in item["path"]
            for item in origin.requests[first_request_count:]
        )
        # DuckDB may return as the relay handler finishes accounting the final
        # local response chunk.  Under scheduler pressure the origin thread can
        # publish its byte count a few milliseconds before the relay thread;
        # require prompt convergence instead of racing those two counters.
        metrics_deadline = time.monotonic() + 1.0
        metrics = stable_http_relay_metrics()
        while (
            metrics["upstream_bytes"] != origin.bytes_sent
            and time.monotonic() < metrics_deadline
        ):
            time.sleep(0.001)
            metrics = stable_http_relay_metrics()
        assert metrics["upstream_bytes"] == origin.bytes_sent
        assert metrics["rejected_requests"] == 0
    finally:
        second_lease.close()
        con.close()


def test_retired_provider_identity_reuse_cannot_serve_prior_duckdb_bytes(
    origin, monkeypatch,
):
    hostname = "objects.relay-test.invalid"
    _map_test_hostname(monkeypatch, hostname)
    identity = "share-cache-v1:" + "9" * 64
    expires_ms = int(time.time() * 1_000) + 60_000
    first = _linked_reflection(
        f"http://{hostname}:{origin.server.server_port}/data?signature=one",
        cache_identity=identity,
        size=len(origin.payload),
        expires_ms=expires_ms,
    )
    first_alias, first_lease = alias_stable_remote_paths(
        first, storage=object(), organization="consumer-org",
    )

    replacement_payload = io.BytesIO()
    pq.write_table(
        pa.table({
            "id": [10_000, 20_000],
            "value": ["new-a", "new-b"],
            "__rowid__": [0, 1],
            "__timestamp__": [2, 2],
        }),
        replacement_payload,
    )
    replacement = _Origin(replacement_payload.getvalue())
    replacement.etag = "etag-v2"
    replacement.start()
    con = duckdb.connect()
    try:
        con.execute("SET enable_external_file_cache=true")
        con.execute("SET enable_http_metadata_cache=true")
        assert con.execute(
            "SELECT count(*), sum(id) FROM read_parquet(?)",
            [first_alias.supers[0].files[0]],
        ).fetchone() == (1_000, 499_500)
        retired_path = first_alias.supers[0].files[0]
        first_lease.close()

        second = _linked_reflection(
            f"http://{hostname}:{replacement.server.server_port}/data"
            "?signature=two",
            cache_identity=identity,
            size=len(replacement.payload),
            expires_ms=expires_ms + 1_000,
            etag=replacement.etag,
        )
        second_alias, second_lease = alias_stable_remote_paths(
            second, storage=object(), organization="consumer-org",
        )
        try:
            replacement_path = second_alias.supers[0].files[0]
            assert replacement_path != retired_path
            assert con.execute(
                "SELECT count(*), sum(id) FROM read_parquet(?)",
                [replacement_path],
            ).fetchone() == (2, 30_000)
            assert replacement.bytes_sent > 0
            assert replacement.requests
        finally:
            second_lease.close()
    finally:
        first_lease.close()
        con.close()
        replacement.close()


def test_linked_identity_is_scoped_to_consumer_policy_and_publication(origin):
    identity = "share-cache-v1:" + "e" * 64
    expires_ms = int(time.time() * 1_000) + 60_000
    url = (
        f"https://objects.example.invalid/data?signature=one"
    )

    def stable_path(
        *, organization="consumer-a", policy="f" * 64,
        row_filter=None, generation=1,
    ):
        reflection = _linked_reflection(
            url,
            cache_identity=identity,
            size=len(origin.payload),
            expires_ms=expires_ms,
            policy_fingerprint=policy,
            row_filter=row_filter,
            publication_generation=generation,
        )
        aliased, lease = alias_stable_remote_paths(
            reflection, storage=object(), organization=organization,
        )
        return aliased.supers[0].files[0], lease

    leases = []
    try:
        baseline, lease = stable_path()
        leases.append(lease)
        same, lease = stable_path()
        leases.append(lease)
        other_consumer, lease = stable_path(organization="consumer-b")
        leases.append(lease)
        other_policy, lease = stable_path(policy="a" * 64)
        leases.append(lease)
        other_row_policy, lease = stable_path(row_filter="tenant_id = 7")
        leases.append(lease)
        other_publication, lease = stable_path(generation=2)
        leases.append(lease)
        assert same != baseline
        assert _relay_object_key(same) == _relay_object_key(baseline)
        assert len({
            _relay_object_key(baseline),
            _relay_object_key(other_consumer),
            _relay_object_key(other_policy),
            _relay_object_key(other_row_policy),
            _relay_object_key(other_publication),
        }) == 5
    finally:
        for lease in leases:
            lease.close()


def test_large_local_manifest_fast_path_does_not_start_relay():
    snapshot = SuperSnapshot(
        super_name="lake",
        simple_name="events",
        simple_version=1,
        files=[f"/data/events/{index}.parquet" for index in range(10_000)],
    )
    reflection = Reflection("local", 0, 10_000, [snapshot])

    aliased, lease = alias_stable_remote_paths(
        reflection, storage=object(), organization="org",
    )
    try:
        assert aliased is reflection
        assert relay_module._RELAY is None
        assert lease._leases == []
    finally:
        lease.close()


def test_execution_relay_proof_is_exact_for_partial_and_all_aliases(origin):
    storage = object()
    size = len(origin.payload)
    expiry = int(time.time() * 1_000) + 60_000
    first_key = "org/lake/events/first.parquet"
    second_key = "org/lake/events/second.parquet"
    first_seal = ResourceObjectSeal(
        size=size, version="version-first", etag=origin.etag,
    )
    second_seal = ResourceObjectSeal(
        size=size, version="version-second", etag=origin.etag,
    )
    bearer_one = (
        f"http://127.0.0.1:{origin.server.server_port}/data?signature=one"
    )
    bearer_two = (
        f"http://127.0.0.1:{origin.server.server_port}/data?signature=two"
    )
    forged = {"forged/key": "local-cache-v1:" + "f" * 64}
    base = _local_reflection(
        bearer_one,
        raw_key=first_key,
        size=size,
        seal=first_seal,
        credential_expires_ms=expiry,
    )
    expected_first = local_resource_cache_identity(
        organization="org",
        storage=storage,
        super_name="lake",
        simple_name="events",
        snapshot_version=7,
        raw_key=first_key,
        size=size,
        object_seal=first_seal,
    )
    expected_second = local_resource_cache_identity(
        organization="org",
        storage=storage,
        super_name="lake",
        simple_name="events",
        snapshot_version=7,
        raw_key=second_key,
        size=size,
        object_seal=second_seal,
    )

    partial_snapshot = dataclass_replace(
        base.supers[0],
        files=[bearer_one, "/data/direct.parquet"],
        resource_keys=[first_key, second_key],
        resource_sizes=[size, size],
        resource_object_seals={first_key: first_seal},
        resource_cache_identities=[None, None],
        resource_credential_generations=[1, None],
        resource_credential_expires_ms=[expiry, None],
        resource_relay_cache_identities=forged,
    )
    partial_input = dataclass_replace(base, supers=[partial_snapshot])
    partial, partial_lease = alias_stable_remote_paths(
        partial_input, storage=storage, organization="org",
    )
    try:
        assert partial.supers[0].files[0].startswith("http://127.0.0.1:")
        assert partial.supers[0].files[1] == "/data/direct.parquet"
        assert partial.supers[0].resource_relay_cache_identities == {
            first_key: expected_first,
        }
        assert partial_input.supers[0].resource_relay_cache_identities == forged
    finally:
        partial_lease.close()

    all_snapshot = dataclass_replace(
        base.supers[0],
        files=[bearer_one, bearer_two],
        resource_keys=[first_key, second_key],
        resource_sizes=[size, size],
        resource_object_seals={
            first_key: first_seal,
            second_key: second_seal,
        },
        resource_cache_identities=[None, None],
        resource_credential_generations=[2, 3],
        resource_credential_expires_ms=[expiry, expiry],
        resource_relay_cache_identities=forged,
    )
    all_input = dataclass_replace(base, supers=[all_snapshot])
    aliased, all_lease = alias_stable_remote_paths(
        all_input, storage=storage, organization="org",
    )
    try:
        assert len(set(aliased.supers[0].files)) == 2
        assert aliased.supers[0].resource_relay_cache_identities == {
            first_key: expected_first,
            second_key: expected_second,
        }
        assert all_input.supers[0].resource_relay_cache_identities == forged
    finally:
        all_lease.close()


def test_execution_relay_proof_clears_injected_map_without_alias():
    snapshot = SuperSnapshot(
        super_name="lake",
        simple_name="events",
        simple_version=1,
        files=["/data/direct.parquet"],
        resource_relay_cache_identities={
            "forged/key": "local-cache-v1:" + "f" * 64,
        },
    )
    reflection = Reflection("local", 0, 1, [snapshot])

    aliased, lease = alias_stable_remote_paths(
        reflection, storage=object(), organization="org",
    )
    try:
        assert aliased is not reflection
        assert aliased.supers[0].files == ["/data/direct.parquet"]
        assert aliased.supers[0].resource_relay_cache_identities == {}
        assert reflection.supers[0].resource_relay_cache_identities
        assert relay_module._RELAY is None
    finally:
        lease.close()


def test_duckdb_stream_holds_isolated_relay_lease_across_rotation(
    origin, monkeypatch, tmp_path,
):
    monkeypatch.setattr(
        engine_common,
        "settings",
        dataclass_replace(
            engine_common.settings,
            SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE="64MB",
            SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE=True,
        ),
    )
    hostname = "objects.relay-test.invalid"
    _map_test_hostname(monkeypatch, hostname)
    identity = "share-cache-v1:" + "4" * 64
    expires_ms = int(time.time() * 1_000) + 60_000
    engine = DuckDB(storage=object(), organization="consumer-org")

    def execute(signature: str, offset: int):
        reflection = _linked_reflection(
            f"http://{hostname}:{origin.server.server_port}/data"
            f"?signature={signature}",
            cache_identity=identity,
            size=len(origin.payload),
            expires_ms=expires_ms + offset,
        )
        parser = SQLParser(
            "shared", "SELECT count(*) AS n, sum(id) AS total FROM events",
            "duckdb",
        )
        manager = QueryPlanManager(
            "shared", f"stable-relay-{signature}", "", parser.original_query,
        )
        manager.temp_dir = str(tmp_path)
        manager.query_plan_path = str(tmp_path / f"{signature}.json")
        return engine.execute_stream(
            reflection, parser, manager, lambda _event: None,
            max_batch_rows=128,
            max_batch_bytes=1024 * 1024,
        )

    try:
        first = execute("one", 0)
        assert stable_http_relay_metrics()["active_routes"] == 1
        first_table = pa.Table.from_batches(list(first))
        assert first_table.to_pylist() == [{"n": 1_000, "total": 499_500}]
        assert stable_http_relay_metrics()["active_routes"] == 0
        first_bytes = origin.bytes_sent
        first_requests = len(origin.requests)
        assert first_bytes > 0

        second = execute("two", 1_000)
        assert stable_http_relay_metrics()["active_routes"] == 1
        second_table = pa.Table.from_batches(list(second))
        assert second_table.to_pylist() == [{"n": 1_000, "total": 499_500}]
        assert stable_http_relay_metrics()["active_routes"] == 0
        assert origin.bytes_sent > first_bytes
        assert len(origin.requests) > first_requests
        assert any(
            "signature=two" in item["path"]
            for item in origin.requests[first_requests:]
        )
    finally:
        engine._reset_connection()


def test_local_presign_identity_requires_seal_and_rotates_on_overwrite(origin):
    raw_key = "org/lake/events/data.parquet"
    storage = object()
    seal_v1 = ResourceObjectSeal(
        size=len(origin.payload), version="version-1", etag=origin.etag,
    )
    first = _local_reflection(
        f"http://127.0.0.1:{origin.server.server_port}/data?signature=one",
        raw_key=raw_key,
        size=len(origin.payload),
        seal=seal_v1,
    )
    second = _local_reflection(
        f"http://127.0.0.1:{origin.server.server_port}/data?signature=two",
        raw_key=raw_key,
        size=len(origin.payload),
        seal=seal_v1,
        credential_generation=2,
    )
    first_alias, first_lease = alias_stable_remote_paths(
        first, storage=storage, organization="org",
    )
    second_alias, second_lease = alias_stable_remote_paths(
        second, storage=storage, organization="org",
    )
    try:
        first_path = first_alias.supers[0].files[0]
        second_path = second_alias.supers[0].files[0]
        assert first_path != second_path
        assert _relay_object_key(first_path) == _relay_object_key(second_path)
    finally:
        first_lease.close()
        second_lease.close()

    overwritten = _local_reflection(
        f"http://127.0.0.1:{origin.server.server_port}/data?signature=three",
        raw_key=raw_key,
        size=len(origin.payload),
        seal=ResourceObjectSeal(
            size=len(origin.payload), version="version-2", etag="etag-v2",
        ),
    )
    overwritten_alias, overwritten_lease = alias_stable_remote_paths(
        overwritten, storage=storage, organization="org",
    )
    try:
        assert _relay_object_key(overwritten_alias.supers[0].files[0]) != (
            _relay_object_key(first_alias.supers[0].files[0])
        )
    finally:
        overwritten_lease.close()

    legacy = _local_reflection(
        f"http://127.0.0.1:{origin.server.server_port}/data?signature=legacy",
        raw_key=raw_key,
        size=len(origin.payload),
        seal=None,
    )
    legacy_alias, legacy_lease = alias_stable_remote_paths(
        legacy, storage=storage, organization="org",
    )
    try:
        assert legacy_alias is legacy
        assert legacy_alias.supers[0].files[0].endswith("signature=legacy")
    finally:
        legacy_lease.close()

    version_only = _local_reflection(
        f"http://127.0.0.1:{origin.server.server_port}/data?signature=version",
        raw_key=raw_key,
        size=len(origin.payload),
        seal=ResourceObjectSeal(size=len(origin.payload), version="version-3"),
    )
    version_alias, version_lease = alias_stable_remote_paths(
        version_only, storage=storage, organization="org",
    )
    try:
        assert version_alias is version_only
        assert version_alias.supers[0].files[0].endswith("signature=version")
    finally:
        version_lease.close()

    unordered = _local_reflection(
        f"http://127.0.0.1:{origin.server.server_port}/data?signature=unordered",
        raw_key=raw_key,
        size=len(origin.payload),
        seal=seal_v1,
        credential_generation=None,
    )
    unordered_alias, unordered_lease = alias_stable_remote_paths(
        unordered, storage=storage, organization="org",
    )
    try:
        assert unordered_alias is unordered
        assert unordered_alias.supers[0].files[0].endswith(
            "signature=unordered"
        )
    finally:
        unordered_lease.close()

    with pytest.raises(
        StableHttpRelayError, match="enforceable object ETag",
    ):
        _get_relay().register(
            "local-cache-v1:" + "9" * 64,
            f"http://127.0.0.1:{origin.server.server_port}/data?signature=x",
            expected_size=len(origin.payload),
            credential_expires_ms=int(time.time() * 1_000) + 60_000,
        )


def test_local_presigned_rotation_isolates_lease_url_and_refetches(
    origin,
):
    raw_key = "org/lake/events/data.parquet"
    storage = object()
    seal = ResourceObjectSeal(
        size=len(origin.payload), version="version-1", etag=origin.etag,
    )
    first = _local_reflection(
        f"http://127.0.0.1:{origin.server.server_port}/data?signature=one",
        raw_key=raw_key,
        size=len(origin.payload),
        seal=seal,
        credential_generation=1,
    )
    first_alias, first_lease = alias_stable_remote_paths(
        first, storage=storage, organization="org",
    )
    stable_path = first_alias.supers[0].files[0]

    con = duckdb.connect()
    try:
        con.execute("SET enable_external_file_cache=true")
        con.execute("SET enable_http_metadata_cache=true")
        assert con.execute(
            "SELECT count(*), sum(id) FROM read_parquet(?)", [stable_path],
        ).fetchone() == (1_000, 499_500)
        first_bytes = origin.bytes_sent
        first_requests = len(origin.requests)
        assert first_bytes > 0
    finally:
        first_lease.close()

    second = _local_reflection(
        f"http://127.0.0.1:{origin.server.server_port}/data?signature=two",
        raw_key=raw_key,
        size=len(origin.payload),
        seal=seal,
        credential_generation=2,
    )
    second_alias, second_lease = alias_stable_remote_paths(
        second, storage=storage, organization="org",
    )
    try:
        second_path = second_alias.supers[0].files[0]
        assert second_path != stable_path
        assert _relay_object_key(second_path) == _relay_object_key(stable_path)
        assert con.execute(
            "SELECT count(*), sum(id) FROM read_parquet(?)", [second_path],
        ).fetchone() == (1_000, 499_500)
        assert origin.bytes_sent > first_bytes
        assert len(origin.requests) > first_requests
        assert any(
            "signature=two" in item["path"]
            for item in origin.requests[first_requests:]
        )
    finally:
        second_lease.close()
        con.close()


def test_linked_relay_rejects_ip_literal_and_missing_identity(origin):
    url = f"http://127.0.0.1:{origin.server.server_port}/data?signature=one"
    linked = _linked_reflection(
        url,
        cache_identity="share-cache-v1:" + "b" * 64,
        size=len(origin.payload),
        expires_ms=int(time.time() * 1_000) + 60_000,
    )
    with pytest.raises(
        StableHttpRelayError, match="allowlisted hostname",
    ):
        alias_stable_remote_paths(
            linked, storage=object(), organization="consumer-org",
        )

    linked.supers[0].resource_cache_identities = []
    with pytest.raises(
        StableHttpRelayError, match="no stable resource identities",
    ):
        alias_stable_remote_paths(
            linked, storage=object(), organization="consumer-org",
        )

    with pytest.raises(
        StableHttpRelayError, match="credential expiry is unavailable",
    ):
        _get_relay().register(
            "share-cache-v1:" + "8" * 64,
            "https://objects.example.invalid/data?signature=one",
            expected_size=len(origin.payload),
            expected_etag=origin.etag,
        )

    with pytest.raises(
        StableHttpRelayError, match="ETag is invalid",
    ):
        _get_relay().register(
            "local-cache-v1:" + "7" * 64,
            f"http://127.0.0.1:{origin.server.server_port}/data?signature=one",
            expected_size=len(origin.payload),
            expected_etag="valid\r\nX-Injected: yes",
            credential_expires_ms=int(time.time() * 1_000) + 60_000,
        )


@pytest.mark.parametrize(
    "hostname",
    ["127.0.0.1", "2130706433", "0177.0.0.1", "0x7f000001", "[::1]"],
)
def test_linked_relay_rejects_alternate_numeric_host_spellings(
    origin, hostname,
):
    with pytest.raises(
        StableHttpRelayError, match="allowlisted hostname",
    ):
        _get_relay().register(
            "share-cache-v1:" + "6" * 64,
            f"http://{hostname}:{origin.server.server_port}/data?signature=one",
            expected_size=len(origin.payload),
            expected_etag=origin.etag,
            credential_expires_ms=int(time.time() * 1_000) + 60_000,
        )


def test_concurrent_linked_credentials_have_isolated_lease_authority(
    origin, monkeypatch,
):
    hostname = "objects.relay-test.invalid"
    _map_test_hostname(monkeypatch, hostname)
    relay = _get_relay()
    identity = "share-cache-v1:" + "c" * 64
    base = f"http://{hostname}:{origin.server.server_port}/data"
    fresh = relay.register(
        identity,
        f"{base}?signature=fresh",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=20_000,
    )
    stale = relay.register(
        identity,
        f"{base}?signature=stale",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=10_000,
    )
    assert stale.url != fresh.url
    assert _relay_object_key(stale.url) == _relay_object_key(fresh.url)
    fresh.close()
    try:
        with pytest.raises(HTTPError) as retired:
            urlopen(Request(fresh.url, method="HEAD"), timeout=1.0)
        _assert_http_error_code(retired, 404)
        with urlopen(stale.url, timeout=5) as response:
            assert response.read() == origin.payload
        assert origin.requests[-1]["path"].endswith("signature=stale")
        assert stable_http_relay_metrics()["active_routes"] == 1
    finally:
        stale.close()
    assert stable_http_relay_metrics()["active_routes"] == 0


def test_linked_equal_expiry_credentials_remain_lease_isolated(
    origin, monkeypatch,
):
    hostname = "objects.relay-test.invalid"
    _map_test_hostname(monkeypatch, hostname)
    relay = _get_relay()
    identity = "share-cache-v1:" + "d" * 64
    base = f"http://{hostname}:{origin.server.server_port}/data"
    expiry = int(time.time() * 1_000) + 60_000
    fresh = relay.register(
        identity,
        f"{base}?signature=fresh",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=expiry,
        share_publication_generation=20,
    )
    stale = relay.register(
        identity,
        f"{base}?signature=stale",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=expiry,
        share_publication_generation=10,
    )
    ambiguous = relay.register(
        identity,
        f"{base}?signature=ambiguous",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=expiry,
        share_publication_generation=20,
    )
    assert len({fresh.url, stale.url, ambiguous.url}) == 3
    assert len({
        _relay_object_key(fresh.url),
        _relay_object_key(stale.url),
        _relay_object_key(ambiguous.url),
    }) == 1
    fresh.close()
    ambiguous.close()
    try:
        with urlopen(stale.url, timeout=5) as response:
            assert response.read() == origin.payload
        assert origin.requests[-1]["path"].endswith("signature=stale")
    finally:
        ambiguous.close()
        stale.close()
    assert stable_http_relay_metrics()["active_routes"] == 0


def test_overlapping_local_credentials_remain_lease_isolated(
    origin,
):
    relay = _get_relay()
    base = f"http://127.0.0.1:{origin.server.server_port}/data"
    now_ms = int(time.time() * 1_000)
    identity = "local-cache-v1:" + "4" * 64
    long_lived = relay.register(
        identity,
        f"{base}?signature=long",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=now_ms + 120_000,
        credential_generation=1,
    )
    short_newer = relay.register(
        identity,
        f"{base}?signature=short",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=now_ms + 60_000,
        credential_generation=2,
    )
    long_lived.close()
    try:
        assert short_newer.url != long_lived.url
        assert _relay_object_key(short_newer.url) == _relay_object_key(
            long_lived.url
        )
        with urlopen(short_newer.url, timeout=5) as response:
            assert response.read() == origin.payload
        assert origin.requests[-1]["path"].endswith("signature=short")
    finally:
        short_newer.close()
    assert stable_http_relay_metrics()["active_routes"] == 0

    tie_identity = "local-cache-v1:" + "5" * 64
    equal_old = relay.register(
        tie_identity,
        f"{base}?signature=equal-old",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=now_ms + 120_000,
        credential_generation=3,
    )
    equal_new = relay.register(
        tie_identity,
        f"{base}?signature=equal-new",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=now_ms + 120_000,
        credential_generation=4,
    )
    equal_new.close()
    try:
        assert equal_old.url != equal_new.url
        with urlopen(equal_old.url, timeout=5) as response:
            assert response.read() == origin.payload
        assert origin.requests[-1]["path"].endswith("signature=equal-old")
    finally:
        equal_old.close()
    assert stable_http_relay_metrics()["active_routes"] == 0


def test_generation_allocation_and_provider_invocation_are_one_keyed_order(
    origin, monkeypatch,
):
    raw_key = "org/lake/events/serialized.parquet"
    seal = ResourceObjectSeal(
        size=len(origin.payload), version="version-1", etag=origin.etag,
    )
    base = f"http://127.0.0.1:{origin.server.server_port}/data"
    source = _local_reflection(
        "s3://bucket/serialized.parquet",
        raw_key=raw_key,
        size=len(origin.payload),
        seal=seal,
        credential_generation=None,
    )
    real_generation = executor_module.next_local_credential_generation
    first_allocation = threading.Event()
    release_first = threading.Event()
    allocations = []
    allocation_lock = threading.Lock()

    def controlled_generation():
        generation = real_generation()
        with allocation_lock:
            allocations.append(generation)
            first = len(allocations) == 1
        if first:
            first_allocation.set()
            if not release_first.wait(timeout=3):
                raise AssertionError("first presign allocation was not released")
        return generation

    monkeypatch.setattr(
        executor_module, "next_local_credential_generation",
        controlled_generation,
    )

    class Storage:
        def __init__(self) -> None:
            self.calls = []
            self.lock = threading.Lock()

        def presign(self, key, *, expiry_seconds):
            assert key == raw_key
            with self.lock:
                self.calls.append(key)
                ordinal = len(self.calls)
            return f"{base}?signature={'older' if ordinal == 1 else 'newer'}"

    storage = Storage()
    refreshed = []
    errors = []

    def refresh() -> None:
        try:
            refreshed.append(_refresh_presigned_reflection(storage, source))
        except BaseException as exc:
            errors.append(exc)

    older_thread = threading.Thread(target=refresh)
    newer_thread = threading.Thread(target=refresh)
    older_thread.start()
    assert first_allocation.wait(timeout=3)
    newer_thread.start()
    time.sleep(0.1)
    # The second worker cannot allocate a generation or invoke the provider
    # while the first is paused between allocation and invocation.
    assert allocations == allocations[:1]
    assert storage.calls == []
    release_first.set()
    older_thread.join(timeout=3)
    newer_thread.join(timeout=3)
    assert not errors
    assert not older_thread.is_alive() and not newer_thread.is_alive()
    assert len(allocations) == 2
    assert storage.calls == [raw_key, raw_key]

    older, newer = sorted(
        refreshed,
        key=lambda item: item.supers[0].resource_credential_generations[0],
    )
    assert older.supers[0].files[0].endswith("signature=older")
    assert newer.supers[0].files[0].endswith("signature=newer")
    newer_alias, newer_lease = alias_stable_remote_paths(
        newer, storage=storage, organization="org",
    )
    older_alias, older_lease = alias_stable_remote_paths(
        older, storage=storage, organization="org",
    )
    newer_lease.close()
    try:
        assert older_alias.supers[0].files[0] != newer_alias.supers[0].files[0]
        assert _relay_object_key(older_alias.supers[0].files[0]) == (
            _relay_object_key(newer_alias.supers[0].files[0])
        )
        with urlopen(older_alias.supers[0].files[0], timeout=5) as response:
            assert response.read() == origin.payload
        assert origin.requests[-1]["path"].endswith("signature=older")
    finally:
        older_lease.close()


def test_out_of_order_concurrent_local_registration_isolates_presigns(
    origin,
):
    raw_key = "org/lake/events/concurrent.parquet"
    seal = ResourceObjectSeal(
        size=len(origin.payload), version="version-1", etag=origin.etag,
    )
    base = f"http://127.0.0.1:{origin.server.server_port}/data"
    source = _local_reflection(
        "s3://bucket/concurrent.parquet",
        raw_key=raw_key,
        size=len(origin.payload),
        seal=seal,
        credential_generation=None,
    )

    class Storage:
        def __init__(self) -> None:
            self._calls = 0
            self._lock = threading.Lock()

        def presign(self, key, *, expiry_seconds):
            assert key == raw_key
            assert expiry_seconds > 0
            with self._lock:
                self._calls += 1
                call = self._calls
            return f"{base}?signature={'older' if call == 1 else 'newer'}"

    storage = Storage()
    newer_registered = threading.Event()
    older_issued = threading.Event()
    leases = {}
    refreshed = {}
    errors = []

    def issue_older_then_register_late() -> None:
        try:
            refreshed["older"] = _refresh_presigned_reflection(
                storage, source,
            )
            older_issued.set()
            if not newer_registered.wait(timeout=3):
                raise AssertionError("newer registration did not complete")
            leases["older"] = alias_stable_remote_paths(
                refreshed["older"], storage=storage, organization="org",
            )
        except BaseException as exc:
            errors.append(exc)
        finally:
            older_issued.set()

    def issue_and_register_newer() -> None:
        try:
            if not older_issued.wait(timeout=3):
                raise AssertionError("older credential was not issued")
            refreshed["newer"] = _refresh_presigned_reflection(
                storage, source,
            )
            leases["newer"] = alias_stable_remote_paths(
                refreshed["newer"], storage=storage, organization="org",
            )
        except BaseException as exc:
            errors.append(exc)
        finally:
            newer_registered.set()

    threads = [
        threading.Thread(target=issue_older_then_register_late),
        threading.Thread(target=issue_and_register_newer),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)

    assert not errors
    assert not any(thread.is_alive() for thread in threads)
    assert (
        refreshed["older"].supers[0].resource_credential_generations[0]
        < refreshed["newer"].supers[0].resource_credential_generations[0]
    )
    newer_reflection, newer_lease = leases["newer"]
    older_reflection, older_lease = leases["older"]
    assert older_reflection.supers[0].files[0] != (
        newer_reflection.supers[0].files[0]
    )
    assert _relay_object_key(older_reflection.supers[0].files[0]) == (
        _relay_object_key(newer_reflection.supers[0].files[0])
    )
    newer_lease.close()
    try:
        with urlopen(
            older_reflection.supers[0].files[0], timeout=5,
        ) as response:
            assert response.read() == origin.payload
        assert origin.requests[-1]["path"].endswith("signature=older")
    finally:
        older_lease.close()


def test_relay_refuses_redirect_bad_range_and_oversized_headers(origin):
    relay = _get_relay()
    identity_prefix = "local-cache-v1:"

    redirect = relay.register(
        identity_prefix + "d" * 64,
        f"http://127.0.0.1:{origin.server.server_port}/redirect",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=int(time.time() * 1_000) + 60_000,
        credential_generation=1,
    )
    try:
        with pytest.raises(HTTPError) as exc:
            urlopen(redirect.url, timeout=5)
        _assert_http_error_code(exc, 502)
        assert [item["path"] for item in origin.requests] == ["/redirect"]
    finally:
        redirect.close()

    bad_range = relay.register(
        identity_prefix + "e" * 64,
        f"http://127.0.0.1:{origin.server.server_port}/data?bad-range=1",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=int(time.time() * 1_000) + 60_000,
        credential_generation=2,
    )
    try:
        request = Request(bad_range.url, headers={"Range": "bytes=0-9"})
        with pytest.raises(HTTPError) as exc:
            urlopen(request, timeout=5)
        _assert_http_error_code(exc, 502)
    finally:
        bad_range.close()

    many_headers = relay.register(
        identity_prefix + "f" * 64,
        f"http://127.0.0.1:{origin.server.server_port}/data?many-headers=1",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=int(time.time() * 1_000) + 60_000,
        credential_generation=3,
    )
    try:
        with pytest.raises(HTTPError) as exc:
            urlopen(many_headers.url, timeout=5)
        _assert_http_error_code(exc, 502)
    finally:
        many_headers.close()

    missing_etag = relay.register(
        identity_prefix + "0" * 64,
        f"http://127.0.0.1:{origin.server.server_port}/data?no-etag=1",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=int(time.time() * 1_000) + 60_000,
        credential_generation=4,
    )
    try:
        with pytest.raises(HTTPError) as exc:
            urlopen(missing_etag.url, timeout=5)
        _assert_http_error_code(exc, 502)
    finally:
        missing_etag.close()


def test_relay_emits_exactly_one_http_status_line(origin):
    relay = _get_relay()
    lease = relay.register(
        "local-cache-v1:" + "1" * 64,
        f"http://127.0.0.1:{origin.server.server_port}/data",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=int(time.time() * 1_000) + 60_000,
        credential_generation=5,
    )
    try:
        parsed = lease.url.split("/", 3)
        host, raw_port = parsed[2].split(":", 1)
        path = "/" + parsed[3]
        with socket.create_connection((host, int(raw_port)), timeout=5) as client:
            client.sendall(
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {host}:{raw_port}\r\n"
                "Range: bytes=0-9\r\n"
                "Connection: close\r\n\r\n".encode("ascii")
            )
            chunks = []
            while True:
                chunk = client.recv(4_096)
                if not chunk:
                    break
                chunks.append(chunk)
        response = b"".join(chunks)
        assert response.count(b"HTTP/1.1 ") == 1
        assert response.startswith(b"HTTP/1.1 206 ")
        assert response.endswith(origin.payload[:10])
    finally:
        lease.close()


def test_relay_metrics_never_expose_identity_or_upstream_url(origin):
    relay = _get_relay()
    identity = "local-cache-v1:" + "2" * 64
    upstream = f"http://127.0.0.1:{origin.server.server_port}/data?secret=yes"
    lease = relay.register(
        identity,
        upstream,
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=int(time.time() * 1_000) + 60_000,
        credential_generation=6,
    )
    try:
        serialized = repr(stable_http_relay_metrics())
        assert identity not in serialized
        assert upstream not in serialized
        assert "secret" not in serialized
    finally:
        lease.close()


def test_request_after_last_lease_is_fail_closed_and_classified_as_retired(
    origin,
):
    relay = _get_relay()
    lease = relay.register(
        "local-cache-v1:" + "7" * 64,
        f"http://127.0.0.1:{origin.server.server_port}/data?signature=retired",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=int(time.time() * 1_000) + 60_000,
        credential_generation=70,
    )
    retired_url = lease.url
    lease.close()

    with pytest.raises(HTTPError) as caught:
        urlopen(Request(retired_url, method="HEAD"), timeout=1.0)
    _assert_http_error_code(caught, 404)
    metrics = stable_http_relay_metrics()
    assert metrics["active_routes"] == 0
    assert metrics["upstream_requests"] == 0
    assert metrics["retired_route_requests"] == 1
    assert metrics["rejected_requests"] == 0

    # An arbitrary opaque-looking route remains an ordinary rejected request;
    # only a route this relay actually retired receives lifecycle classification.
    unknown_url = retired_url.rsplit("/", 1)[0] + "/" + "f" * 64
    with pytest.raises(HTTPError) as unknown:
        urlopen(Request(unknown_url, method="HEAD"), timeout=1.0)
    _assert_http_error_code(unknown, 404)
    metrics = stable_http_relay_metrics()
    assert metrics["retired_route_requests"] == 1
    assert metrics["rejected_requests"] == 1


def test_large_route_set_uses_bounded_bulk_teardown(origin):
    relay = _get_relay()
    expires_ms = int(time.time() * 1_000) + 60_000
    upstream = (
        f"http://127.0.0.1:{origin.server.server_port}/data?signature=bulk"
    )
    leases = [
        relay.register(
            "local-cache-v1:" + f"{index:064x}",
            upstream,
            expected_size=len(origin.payload),
            expected_etag=origin.etag,
            credential_expires_ms=expires_ms,
            credential_generation=index + 1,
        )
        for index in range(10_000)
    ]
    assert stable_http_relay_metrics()["active_routes"] == 10_000

    started = time.monotonic()
    relay_module.StableRelayLease(leases).close()
    elapsed = time.monotonic() - started

    assert stable_http_relay_metrics()["active_routes"] == 0
    assert len(relay._retired_routes) <= relay_module._MAX_RETIRED_ROUTE_MARKERS
    assert len(relay._deadline_heap) <= 1_024
    # The former per-route full tombstone scan took about 6.8 seconds at this
    # size on the audit host. Leave broad CI headroom while pinning the linear,
    # one-lock lifecycle rather than an exact microbenchmark result.
    assert elapsed < 3.0


def test_live_route_registry_fails_closed_at_capacity(origin, monkeypatch):
    monkeypatch.setattr(relay_module, "_MAX_LIVE_RELAY_ROUTES", 2)
    relay = _get_relay()
    expires_ms = int(time.time() * 1_000) + 60_000
    upstream = f"http://127.0.0.1:{origin.server.server_port}/data?token=x"
    leases = []
    try:
        for index in range(2):
            leases.append(relay.register(
                "local-cache-v1:" + f"{index + 100:064x}",
                upstream,
                expected_size=len(origin.payload),
                expected_etag=origin.etag,
                credential_expires_ms=expires_ms,
                credential_generation=index + 1,
            ))
        with pytest.raises(
            StableHttpRelayError, match="route capacity is exhausted",
        ):
            relay.register(
                "local-cache-v1:" + f"{102:064x}",
                upstream,
                expected_size=len(origin.payload),
                expected_etag=origin.etag,
                credential_expires_ms=expires_ms,
                credential_generation=3,
            )
    finally:
        relay_module.StableRelayLease(leases).close()


def test_duplicate_route_leases_fail_closed_at_boundary_capacity(
    origin, monkeypatch,
):
    monkeypatch.setattr(relay_module, "_MAX_LIVE_RELAY_BOUNDARIES", 2)
    relay = _get_relay()
    expires_ms = int(time.time() * 1_000) + 60_000
    upstream = f"http://127.0.0.1:{origin.server.server_port}/data?token=x"
    identity = "local-cache-v1:" + "2" * 64
    leases = []
    try:
        for generation in (1, 2):
            leases.append(relay.register(
                identity,
                upstream,
                expected_size=len(origin.payload),
                expected_etag=origin.etag,
                credential_expires_ms=expires_ms,
                credential_generation=generation,
            ))
        with pytest.raises(
            StableHttpRelayError, match="lease capacity is exhausted",
        ):
            relay.register(
                identity,
                upstream,
                expected_size=len(origin.payload),
                expected_etag=origin.etag,
                credential_expires_ms=expires_ms,
                credential_generation=3,
            )
    finally:
        relay_module.StableRelayLease(leases).close()


def test_retired_nominal_identity_rotates_route_on_etag_change(origin):
    relay = _get_relay()
    identity = "local-cache-v1:" + "3" * 64
    expires_ms = int(time.time() * 1_000) + 60_000
    upstream = f"http://127.0.0.1:{origin.server.server_port}/data?token=x"
    first = relay.register(
        identity,
        upstream,
        expected_size=len(origin.payload),
        expected_etag="etag-v1",
        credential_expires_ms=expires_ms,
        credential_generation=1,
    )
    first_path = first.url
    first.close()
    second = relay.register(
        identity,
        upstream,
        expected_size=len(origin.payload),
        expected_etag="etag-v2",
        credential_expires_ms=expires_ms + 1_000,
        credential_generation=2,
    )
    try:
        assert second.url != first_path
    finally:
        second.close()


def test_shutdown_erases_active_route_and_old_lease_is_idempotent(origin):
    relay = _get_relay()
    lease = relay.register(
        "local-cache-v1:" + "3" * 64,
        f"http://127.0.0.1:{origin.server.server_port}/data?signature=one",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=int(time.time() * 1_000) + 60_000,
        credential_generation=7,
    )
    assert stable_http_relay_metrics()["active_routes"] == 1
    shutdown_stable_http_relay()
    lease.close()
    # Diagnostics recreate a fresh process-secret/server with no retained URL.
    assert stable_http_relay_metrics()["active_routes"] == 0


def _relay_socket_target(url: str) -> tuple[str, int, str]:
    parts = url.split("/", 3)
    host, raw_port = parts[2].split(":", 1)
    return host, int(raw_port), "/" + parts[3]


def test_partial_header_connection_is_closed_on_short_read_deadline(
    origin, monkeypatch,
):
    monkeypatch.setattr(relay_module, "_HEADER_READ_TIMEOUT_SECONDS", 0.15)
    relay = _get_relay()
    lease = relay.register(
        "local-cache-v1:" + "5" * 64,
        f"http://127.0.0.1:{origin.server.server_port}/data?signature=one",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=int(time.time() * 1_000) + 60_000,
        credential_generation=8,
    )
    host, port, path = _relay_socket_target(lease.url)
    try:
        with socket.create_connection((host, port), timeout=1) as client:
            client.settimeout(1)
            client.sendall(
                f"GET {path} HTTP/1.1\r\n".encode("ascii")
            )
            started = time.monotonic()
            response = None
            # Keep every inter-byte gap below the socket inactivity timeout.
            # Only an absolute header deadline can stop this slowloris pattern.
            for byte in f"Host: {host}".encode("ascii"):
                try:
                    client.sendall(bytes([byte]))
                except (BrokenPipeError, ConnectionResetError):
                    response = b""
                    break
                time.sleep(0.03)
                readable, _, _ = select.select([client], [], [], 0)
                if readable:
                    try:
                        response = client.recv(1_024)
                    except ConnectionResetError:
                        response = b""
                    break
            if response is None:
                try:
                    response = client.recv(1_024)
                except ConnectionResetError:
                    response = b""
            elapsed = time.monotonic() - started
        assert response == b""
        assert elapsed < 0.5
        assert origin.requests == []
        assert stable_http_relay_metrics()["upstream_requests"] == 0
    finally:
        lease.close()


def test_saturated_relay_closes_before_route_lookup_without_existence_oracle(
    origin, monkeypatch,
):
    monkeypatch.setattr(relay_module, "_MAX_RELAY_CONNECTIONS", 2)
    monkeypatch.setattr(relay_module, "_HEADER_READ_TIMEOUT_SECONDS", 5.0)
    relay = _get_relay()
    lease = relay.register(
        "local-cache-v1:" + "a" * 64,
        f"http://127.0.0.1:{origin.server.server_port}/data?signature=one",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=int(time.time() * 1_000) + 60_000,
        credential_generation=9,
    )
    host, port, valid_path = _relay_socket_target(lease.url)
    holders = []
    try:
        for _ in range(2):
            client = socket.create_connection((host, port), timeout=1)
            client.settimeout(1)
            client.sendall(b"GET /partial HTTP/1.1\r\nHost: relay")
            holders.append(client)

        deadline = time.monotonic() + 1.0
        while relay._server._connection_slots._value != 0:
            if time.monotonic() >= deadline:
                pytest.fail("relay handler slots did not saturate")
            time.sleep(0.01)

        def saturated_response(path: str) -> bytes:
            with socket.create_connection((host, port), timeout=1) as client:
                client.settimeout(1)
                try:
                    client.sendall(
                        f"GET {path} HTTP/1.1\r\n"
                        f"Host: {host}:{port}\r\n\r\n".encode("ascii")
                    )
                    return client.recv(1_024)
                except (BrokenPipeError, ConnectionResetError):
                    return b""

        valid_response = saturated_response(valid_path)
        invalid_response = saturated_response("/v1/" + "0" * 64)
        assert valid_response == invalid_response == b""
        assert origin.requests == []
        assert stable_http_relay_metrics()["upstream_requests"] == 0
    finally:
        for client in holders:
            client.close()
        lease.close()


def test_raw_header_bytes_are_rejected_before_stdlib_can_buffer_large_values(
    origin, monkeypatch,
):
    monkeypatch.setattr(relay_module, "_HEADER_READ_TIMEOUT_SECONDS", 5.0)
    relay = _get_relay()
    lease = relay.register(
        "local-cache-v1:" + "b" * 64,
        f"http://127.0.0.1:{origin.server.server_port}/data?signature=one",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=int(time.time() * 1_000) + 60_000,
        credential_generation=10,
    )
    host, port, path = _relay_socket_target(lease.url)
    oversized_value = b"x" * (relay_module._MAX_REQUEST_HEADER_BYTES + 1_024)
    try:
        with socket.create_connection((host, port), timeout=1) as client:
            client.settimeout(1)
            client.sendall(
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {host}:{port}\r\n"
                "X-Large: ".encode("ascii")
                + oversized_value
                + b"\r\n\r\n"
            )
            chunks = []
            while True:
                try:
                    chunk = client.recv(4_096)
                except ConnectionResetError:
                    break
                if not chunk:
                    break
                chunks.append(chunk)
        response = b"".join(chunks)
        assert response.count(b"HTTP/1.1 ") == 1
        assert response.startswith(b"HTTP/1.1 431 ")
        assert origin.requests == []
        assert stable_http_relay_metrics()["upstream_requests"] == 0
    finally:
        lease.close()


@pytest.mark.parametrize("boundary", ["deadline", "cancel", "final-lease"])
def test_slow_drip_transfer_is_hard_bounded_and_handler_slot_is_reusable(
    origin, monkeypatch, boundary,
):
    monkeypatch.setattr(relay_module, "_MAX_RELAY_CONNECTIONS", 1)
    relay = _get_relay()
    cancel_event = threading.Event()
    deadline = time.monotonic() + (0.75 if boundary == "deadline" else 5.0)
    expiry = int(time.time() * 1_000) + 60_000
    slow = relay.register(
        "local-cache-v1:" + "c" * 64,
        f"http://127.0.0.1:{origin.server.server_port}"
        "/data?slow-drip=1",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=expiry,
        credential_generation=100,
        deadline_monotonic=deadline,
        cancel_event=cancel_event,
    )
    outcome = {}

    def read_slowly() -> None:
        try:
            with urlopen(slow.url, timeout=2) as response:
                outcome["body"] = response.read()
        except BaseException as exc:
            _record_thread_error(outcome, exc)

    reader = threading.Thread(target=read_slowly)
    reader.start()
    assert origin.slow_drip_started.wait(timeout=2)
    started = time.monotonic()
    if boundary == "cancel":
        time.sleep(0.05)
        cancel_event.set()
    elif boundary == "final-lease":
        time.sleep(0.05)
        slow.close()
    reader.join(timeout=1.25)
    elapsed = time.monotonic() - started
    assert not reader.is_alive()
    assert "error" in outcome or len(outcome.get("body", b"")) < len(
        origin.payload
    )
    assert elapsed < 1.25

    slot_deadline = time.monotonic() + 1.0
    while relay._server._connection_slots._value != 1:
        if time.monotonic() >= slot_deadline:
            pytest.fail("relay handler slot remained occupied after transfer stop")
        time.sleep(0.01)
    slow.close()

    # A fresh route can use the only handler slot immediately after the
    # deadline/cancel/final-lease boundary tears down the slow upstream read.
    fresh = relay.register(
        "local-cache-v1:" + "d" * 64,
        f"http://127.0.0.1:{origin.server.server_port}/data?fresh=1",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=expiry,
        credential_generation=101,
        deadline_monotonic=time.monotonic() + 2.0,
    )
    try:
        with urlopen(fresh.url, timeout=2) as response:
            assert response.read() == origin.payload
    finally:
        fresh.close()


@pytest.mark.parametrize("boundary", ["deadline", "cancel"])
def test_slow_drip_upstream_headers_cannot_outlive_open_boundary_or_slot(
    origin, monkeypatch, boundary,
):
    monkeypatch.setattr(relay_module, "_MAX_RELAY_CONNECTIONS", 1)
    relay = _get_relay()
    cancel_event = threading.Event()
    deadline = time.monotonic() + (0.5 if boundary == "deadline" else 5.0)
    expiry = int(time.time() * 1_000) + 60_000
    slow = relay.register(
        "local-cache-v1:" + "f" * 64,
        f"http://127.0.0.1:{origin.server.server_port}"
        "/data?slow-headers=1",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=expiry,
        credential_generation=300,
        deadline_monotonic=deadline,
        cancel_event=cancel_event,
    )
    outcome = {}

    def request_slow_headers() -> None:
        try:
            with urlopen(slow.url, timeout=2) as response:
                outcome["body"] = response.read()
        except BaseException as exc:
            _record_thread_error(outcome, exc)

    reader = threading.Thread(target=request_slow_headers)
    reader.start()
    assert origin.slow_headers_started.wait(timeout=1)
    assert stable_http_relay_metrics()["active_upstream_opens"] == 1
    started = time.monotonic()
    if boundary == "cancel":
        time.sleep(0.1)
        cancel_event.set()
    reader.join(timeout=1.0)
    assert not reader.is_alive()
    assert "error" in outcome
    assert time.monotonic() - started < 1.0

    slot_deadline = time.monotonic() + 0.75
    while (
        relay._server._connection_slots._value != 1
        or stable_http_relay_metrics()["active_upstream_opens"] != 0
    ):
        if time.monotonic() >= slot_deadline:
            pytest.fail(
                "pre-response open retained physical accounting or handler slot"
            )
        time.sleep(0.01)
    slow.close()

    # No opener worker is abandoned: the tracked connection is closed in the
    # original bounded handler, and the only handler slot can immediately
    # complete another origin request.
    fresh = relay.register(
        "local-cache-v1:" + "0" * 64,
        f"http://127.0.0.1:{origin.server.server_port}/data?fresh=headers",
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=expiry,
        credential_generation=301,
        deadline_monotonic=time.monotonic() + 2.0,
    )
    try:
        with urlopen(fresh.url, timeout=2) as response:
            assert response.read() == origin.payload
    finally:
        fresh.close()


@pytest.mark.parametrize("short_boundary", ["deadline", "cancel"])
def test_releasing_inactive_boundary_does_not_revoke_other_lease(
    origin, short_boundary,
):
    relay = _get_relay()
    identity = "local-cache-v1:" + "a" * 64
    upstream = f"http://127.0.0.1:{origin.server.server_port}/data"
    expiry = int(time.time() * 1_000) + 60_000
    cancel_event = threading.Event()
    short = relay.register(
        identity,
        upstream,
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=expiry,
        credential_generation=400,
        deadline_monotonic=time.monotonic() + (
            0.05 if short_boundary == "deadline" else 30.0
        ),
        cancel_event=cancel_event,
    )
    long = relay.register(
        identity,
        upstream,
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=expiry,
        credential_generation=401,
        deadline_monotonic=time.monotonic() + 30.0,
    )
    assert short.url != long.url
    assert _relay_object_key(short.url) == _relay_object_key(long.url)

    # Hold the registry lock so the watchdog cannot win this race. close()
    # must itself observe an already-inactive boundary instead of deleting the
    # evidence; the independently authenticated long lease remains live.
    with relay._route_condition:
        if short_boundary == "cancel":
            cancel_event.set()
        else:
            time.sleep(0.075)
        short.close()

    assert stable_http_relay_metrics()["active_routes"] == 1
    with pytest.raises(HTTPError) as retired:
        urlopen(Request(short.url, method="HEAD"), timeout=1.0)
    _assert_http_error_code(retired, 404)
    with urlopen(Request(long.url, method="HEAD"), timeout=1.0) as response:
        assert int(response.headers["Content-Length"]) == len(origin.payload)
    long.close()
    assert stable_http_relay_metrics()["active_routes"] == 0


@pytest.mark.parametrize("short_boundary", ["deadline", "cancel"])
def test_short_lease_boundary_does_not_interrupt_isolated_long_transfer(
    origin, monkeypatch, short_boundary,
):
    monkeypatch.setattr(relay_module, "_MAX_RELAY_CONNECTIONS", 1)
    relay = _get_relay()
    base = (
        f"http://127.0.0.1:{origin.server.server_port}/data?slow-drip=1"
    )
    identity = "local-cache-v1:" + "e" * 64
    expiry = int(time.time() * 1_000) + 60_000
    short_cancel = threading.Event()
    long_cancel = threading.Event()
    long_deadline = time.monotonic() + 30.0
    short = relay.register(
        identity,
        base,
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=expiry,
        credential_generation=200,
        deadline_monotonic=time.monotonic() + (
            0.2 if short_boundary == "deadline" else 5.0
        ),
        cancel_event=short_cancel,
    )
    long = relay.register(
        identity,
        base,
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=expiry,
        credential_generation=201,
        deadline_monotonic=long_deadline,
        cancel_event=long_cancel,
    )
    assert short.url != long.url
    assert _relay_object_key(short.url) == _relay_object_key(long.url)
    outcome = {}

    def read_slowly() -> None:
        try:
            with urlopen(long.url, timeout=2) as response:
                outcome["body"] = response.read()
        except BaseException as exc:
            _record_thread_error(outcome, exc)

    reader = threading.Thread(target=read_slowly)
    reader.start()
    assert origin.slow_drip_started.wait(timeout=2)
    if short_boundary == "cancel":
        short_cancel.set()
        time.sleep(0.1)
    else:
        time.sleep(0.3)

    # The short route is gone, but its boundary cannot reach the real transfer
    # authenticated by the long lease's distinct URL.
    assert reader.is_alive()
    assert not long_cancel.is_set()
    assert time.monotonic() < long_deadline
    assert stable_http_relay_metrics()["active_routes"] == 1
    assert relay._server._connection_slots._value == 0

    long_cancel.set()
    reader.join(timeout=0.75)
    assert not reader.is_alive()
    assert "error" in outcome or len(outcome.get("body", b"")) < len(
        origin.payload
    )

    slot_deadline = time.monotonic() + 0.75
    while relay._server._connection_slots._value != 1:
        if time.monotonic() >= slot_deadline:
            pytest.fail("long-lease cancellation did not release handler slot")
        time.sleep(0.01)

    # A future query for the same immutable object gets another lease token;
    # neither retired query URL can borrow its authority.
    replacement = relay.register(
        identity,
        base,
        expected_size=len(origin.payload),
        expected_etag=origin.etag,
        credential_expires_ms=expiry,
        credential_generation=202,
        deadline_monotonic=time.monotonic() + 2.0,
    )
    try:
        assert len({short.url, long.url, replacement.url}) == 3
        for retired_url in (short.url, long.url):
            with pytest.raises(HTTPError) as retired:
                urlopen(Request(retired_url, method="HEAD"), timeout=1.0)
            _assert_http_error_code(retired, 404)
            release_deadline = time.monotonic() + 0.5
            while relay._server._connection_slots._value != 1:
                if time.monotonic() >= release_deadline:
                    pytest.fail("retired lease request retained handler slot")
                time.sleep(0.01)
        with urlopen(
            Request(replacement.url, method="HEAD"), timeout=1.0,
        ) as response:
            assert int(response.headers["Content-Length"]) == len(
                origin.payload
            )
    finally:
        replacement.close()
        short.close()
        long.close()
    assert stable_http_relay_metrics()["active_routes"] == 0
