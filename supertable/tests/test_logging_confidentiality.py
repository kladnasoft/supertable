"""Confidentiality regressions for generic HTTP request diagnostics."""

from __future__ import annotations

import logging
import re

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from supertable.logging import CORRELATION_HEADER, RequestLoggingMiddleware


def _captured_records(caplog) -> str:
    return "\n".join(
        f"{record.getMessage()} {record.__dict__!r}"
        for record in caplog.records
        if record.name.startswith("supertable.")
    )


def test_request_logging_never_retains_path_or_untrusted_header_secrets(caplog):
    async def item(_request):
        return PlainTextResponse("ok")

    app = Starlette(routes=[Route("/items/{item_id}", item)])
    app.add_middleware(RequestLoggingMiddleware, service="confidentiality-test")

    path_secret = "SIGNED_PATH_SUPERSECRET"
    correlation_secret = "Bearer CORRELATION_SUPERSECRET"
    forwarded_secret = "FORWARDED_FOR_SUPERSECRET"
    caplog.set_level(logging.INFO, logger="supertable.confidentiality-test.access")

    with TestClient(app) as client:
        matched = client.get(
            f"/items/{path_secret}",
            headers={
                CORRELATION_HEADER: correlation_secret,
                "X-Forwarded-For": forwarded_secret,
            },
        )
        unmatched = client.get(f"/missing/{path_secret}")

    assert matched.status_code == 200
    assert unmatched.status_code == 404
    generated = matched.headers[CORRELATION_HEADER]
    assert generated != correlation_secret
    assert re.fullmatch(r"[0-9a-f]{12}", generated)

    captured = _captured_records(caplog)
    assert path_secret not in captured
    assert correlation_secret not in captured
    assert forwarded_secret not in captured
    assert "<request-path bytes=" in captured
    assert any(
        record.__dict__.get("client_ip") == "-" for record in caplog.records
    )


def test_request_logging_preserves_one_valid_bounded_correlation_id(caplog):
    async def health(_request):
        return PlainTextResponse("ok")

    app = Starlette(routes=[Route("/ready", health)])
    app.add_middleware(RequestLoggingMiddleware, service="correlation-test")
    caplog.set_level(logging.INFO, logger="supertable.correlation-test.access")

    with TestClient(app) as client:
        response = client.get(
            "/ready", headers={CORRELATION_HEADER: "caller-safe_id:7"},
        )

    assert response.headers[CORRELATION_HEADER] == "caller-safe_id:7"
    assert any(
        record.__dict__.get("correlation_id") == "caller-safe_id:7"
        for record in caplog.records
    )
