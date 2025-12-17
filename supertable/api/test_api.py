from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api_app_v20251216_1720_v1api import router


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    return app


def test_v1_routes_exist() -> None:
    app = _app()
    paths = {(r.path, tuple(sorted(r.methods or []))) for r in app.router.routes}  # type: ignore[attr-defined]

    expected = [
        ("/api/v1/supers", ("GET",)),
        ("/api/v1/super", ("GET",)),
        ("/api/v1/tables", ("GET",)),
        ("/api/v1/schema", ("GET",)),
        ("/api/v1/stats", ("GET",)),
        ("/api/v1/table/{simple_name}", ("GET",)),
        ("/api/v1/mirrors", ("GET",)),
        ("/api/v1/mirrors/enable", ("POST",)),
        ("/api/v1/mirrors/disable", ("POST",)),
        ("/api/v1/rbac/users", ("GET",)),
        ("/api/v1/rbac/users/{user_hash}", ("GET",)),
        ("/api/v1/rbac/roles", ("GET",)),
        ("/api/v1/rbac/roles/{role_hash}", ("GET",)),
        ("/api/v1/supers/{super_name}", ("DELETE",)),
        ("/api/v1/supers/{super_name}/tables/{table}", ("DELETE",)),
        ("/api/v1/execute", ("POST",)),
        ("/api/v1/estimate", ("GET",)),
        ("/api/v1/healthz", ("GET",)),
        ("/healthz", ("GET",)),
    ]

    for p, methods in expected:
        assert any(route_path == p and all(m in route_methods for m in methods) for route_path, route_methods in paths)


def test_healthz_works() -> None:
    client = TestClient(_app())
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json().get("ok") is True

    resp2 = client.get("/api/v1/healthz")
    assert resp2.status_code == 200
    assert resp2.json().get("ok") is True
