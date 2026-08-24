# route: supertable.audit.tests.test_middleware_security
"""Security regressions for the automatic HTTP audit middleware."""
from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest
from starlette.requests import Request
from starlette.responses import StreamingResponse

import supertable.audit as audit_pkg
from supertable.audit.middleware import AuditMiddleware, _extract_org


def _request(query_string: bytes = b"organization=acme") -> Request:
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": "https",
            "path": "/api/v1/query",
            "raw_path": b"/api/v1/query",
            "query_string": query_string,
            "headers": [],
            "client": ("192.0.2.10", 12345),
            "server": ("example.test", 443),
        }
    )


def _middleware() -> AuditMiddleware:
    async def _unused_app(scope, receive, send):  # pragma: no cover
        raise AssertionError("the wrapped ASGI app must not be invoked directly")

    return AuditMiddleware(_unused_app, server="api")


def test_unhandled_exception_records_only_its_class() -> None:
    middleware = _middleware()
    recorded: list[tuple[int, str]] = []
    middleware._emit_error_event = (  # type: ignore[method-assign]
        lambda request, status, error_type, start_ms: recorded.append(
            (status, error_type)
        )
    )

    async def _raise(_request: Request):
        raise RuntimeError("redis://admin:top-secret@example.test/0")

    with pytest.raises(RuntimeError, match="top-secret"):
        asyncio.run(middleware.dispatch(_request(), _raise))

    assert recorded == [(500, "RuntimeError")]


def test_server_error_does_not_consume_or_copy_streamed_response_body() -> None:
    middleware = _middleware()
    recorded: list[tuple[int, str]] = []
    middleware._emit_error_event = (  # type: ignore[method-assign]
        lambda request, status, error_type, start_ms: recorded.append(
            (status, error_type)
        )
    )
    body_started = False

    async def _secret_body():
        nonlocal body_started
        body_started = True
        yield b'{"detail":"token=top-secret"}'

    async def _respond(_request: Request):
        return StreamingResponse(_secret_body(), status_code=503)

    response = asyncio.run(middleware.dispatch(_request(), _respond))

    assert response.status_code == 503
    assert body_started is False
    assert recorded == [(503, "HTTPServerError")]


def test_error_audit_detail_contains_reference_but_no_exception_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict = {}
    monkeypatch.setattr(
        audit_pkg,
        "emit",
        lambda **kwargs: captured.update(kwargs),
        raising=True,
    )

    request = _request()
    request.state.session_org = "acme"
    request.state.correlation_id = "corr-123"
    middleware = _middleware()
    middleware._emit_error_event(request, 500, "RuntimeError", 1_700_000_000_000)

    detail = json.loads(captured["detail"])
    assert detail["error_type"] == "RuntimeError"
    assert len(detail["error_ref"]) == 24
    assert "error" not in detail
    assert "top-secret" not in captured["detail"]


def test_query_selector_cannot_choose_audit_tenant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import supertable.config.settings as settings_module

    monkeypatch.setattr(
        settings_module,
        "settings",
        SimpleNamespace(SUPERTABLE_ORGANIZATION="deployment-org"),
    )
    request = _request(b"organization=attacker-org&org=second-attacker")

    assert _extract_org(request) == "deployment-org"

    request.state.session_org = "authenticated-org"
    assert _extract_org(request) == "authenticated-org"
