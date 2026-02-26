from __future__ import annotations

import importlib
import os
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


def _set_test_auth_env() -> None:
    # Router constructs auth dependency at import time, so env must exist before import.
    os.environ.setdefault("SUPERTABLE_AUTH_MODE", "api_key")
    os.environ.setdefault("SUPERTABLE_API_KEY", "test-api-key")
    os.environ.setdefault("SUPERTABLE_AUTH_HEADER_NAME", "X-API-Key")


def _import_api_module():
    _set_test_auth_env()
    return importlib.import_module("supertable.api.api")


def _app(monkeypatch: pytest.MonkeyPatch) -> FastAPI:
    api_mod = _import_api_module()

    # ---- Patch external dependencies so endpoints are testable in isolation ----
    class _Timer:
        timings = {"total_ms": 1}

    class _PlanStats:
        stats = {"rows_scanned": 0}

    class FakeDataReader:
        def __init__(self, super_name: str, organization: str, query: str) -> None:
            self.super_name = super_name
            self.organization = organization
            self.query = query
            self.timer = _Timer()
            self.plan_stats = _PlanStats()

        def execute(self, user_hash: str, with_scan: bool, engine: Any):
            # Return list-of-lists so router's preview fallback path is exercised.
            df = [[1, 2], [3, 4]]
            meta1 = {"user_hash": user_hash, "with_scan": with_scan, "engine": str(engine)}
            meta2 = {"ok": True}
            return df, meta1, meta2


    class FakeDataWriter:
        def __init__(self, super_name: str, organization: str) -> None:
            self.super_name = super_name
            self.organization = organization

        def write(self, user_hash: str, simple_name: str, data: Any, overwrite_columns: list[str]):
            # Return deterministic values
            return ["a", "b"], 5, 5, 0

    class FakeStaging:
        def __init__(self, *, organization: str, super_name: str, staging_name: str) -> None:
            self.organization = organization
            self.super_name = super_name
            self.staging_name = staging_name

        def save_as_parquet(self, *, arrow_table: Any, base_file_name: str) -> str:
            return f"{base_file_name}_123.parquet"

    def fake_read_arrow_table_from_upload(upload):
        return {"fake": "table"}

    class FakeMetaReader:
        def __init__(self, organization: str, super_name: str) -> None:
            self.organization = organization
            self.super_name = super_name

        def get_super_meta(self, user_hash: str):
            return {
                "organization": self.organization,
                "super_name": self.super_name,
                "user_hash": user_hash,
            }

        def get_table_schema(self, table: str, user_hash: str | None):
            return [{"col": "String", "table": table, "user_hash": user_hash}]

        def get_table_stats(self, table: str, user_hash: str | None):
            return {"table": table, "user_hash": user_hash, "rows": 2}

    def fake_list_supers(*, organization: str):
        return ["super_a", "super_b"]

    def fake_list_tables(*, organization: str, super_name: str):
        return ["table_a", "table_b"]

    # Patch symbols imported/used by the router module.
    monkeypatch.setattr(api_mod, "DataReader", FakeDataReader, raising=True)
    monkeypatch.setattr(api_mod, "MetaReader", FakeMetaReader, raising=True)
    monkeypatch.setattr(api_mod, "list_supers", fake_list_supers, raising=True)
    monkeypatch.setattr(api_mod, "list_tables", fake_list_tables, raising=True)

    monkeypatch.setattr(api_mod, "DataWriter", FakeDataWriter, raising=False)
    monkeypatch.setattr(api_mod, "Staging", FakeStaging, raising=False)
    monkeypatch.setattr(api_mod, "_read_arrow_table_from_upload", fake_read_arrow_table_from_upload, raising=False)

    app = FastAPI()
    app.include_router(api_mod.router)
    return app


def _auth_headers() -> dict[str, str]:
    return {"X-API-Key": os.environ["SUPERTABLE_API_KEY"]}


def test_v1_routes_exist(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app(monkeypatch)
    paths = {(r.path, tuple(sorted(r.methods or []))) for r in app.router.routes}  # type: ignore[attr-defined]

    expected = [
        ("/api/v1/supers", ("GET",)),
        ("/api/v1/tables", ("GET",)),
        ("/api/v1/super", ("GET",)),
        ("/api/v1/schema", ("GET",)),
        ("/api/v1/stats", ("GET",)),
        ("/api/v1/table/{simple_name}", ("GET",)),
        ("/api/v1/execute", ("POST",)),
        ("/api/v1/write", ("POST",)),
        ("/api/v1/stage/upload", ("POST",)),
        ("/healthz", ("GET",)),
    ]

    for p, methods in expected:
        assert any(
            route_path == p and all(m in route_methods for m in methods)
            for route_path, route_methods in paths
        )


def test_healthz_works_without_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    client = TestClient(_app(monkeypatch))
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json().get("ok") is True


def test_all_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    client = TestClient(_app(monkeypatch))
    headers = _auth_headers()

    # GET /api/v1/supers
    r = client.get("/api/v1/supers", params={"organization": "org1"}, headers=headers)
    assert r.status_code == 200
    assert r.json().get("ok") is True

    # GET /api/v1/tables
    r = client.get(
        "/api/v1/tables",
        params={"organization": "org1", "super_name": "super_a"},
        headers=headers,
    )
    assert r.status_code == 200
    assert r.json().get("ok") is True

    # GET /api/v1/super
    r = client.get(
        "/api/v1/super",
        params={"organization": "org1", "super_name": "super_a", "user_hash": "u1"},
        headers=headers,
    )
    assert r.status_code == 200
    assert r.json().get("ok") is True

    # GET /api/v1/schema
    r = client.get(
        "/api/v1/schema",
        params={
            "organization": "org1",
            "super_name": "super_a",
            "table": "table_a",
            "user_hash": "u1",
        },
        headers=headers,
    )
    assert r.status_code == 200
    assert r.json().get("ok") is True

    # GET /api/v1/stats
    r = client.get(
        "/api/v1/stats",
        params={
            "organization": "org1",
            "super_name": "super_a",
            "table": "table_a",
            "user_hash": "u1",
        },
        headers=headers,
    )
    assert r.status_code == 200
    assert r.json().get("ok") is True

    # GET /api/v1/table/{simple_name} (default meta mode)
    r = client.get(
        "/api/v1/table/table_a",
        params={"org": "org1", "sup": "super_a"},
        headers=headers,
    )
    assert r.status_code == 200
    assert r.json().get("ok") is True

    # GET /api/v1/table/{simple_name} (schema mode)
    r = client.get(
        "/api/v1/table/table_a",
        params={"org": "org1", "sup": "super_a", "mode": "schema", "user_hash": "u1"},
        headers=headers,
    )
    assert r.status_code == 200
    assert r.json().get("mode") == "schema"

    # GET /api/v1/table/{simple_name} (stats mode)
    r = client.get(
        "/api/v1/table/table_a",
        params={"org": "org1", "sup": "super_a", "mode": "stats", "user_hash": "u1"},
        headers=headers,
    )
    assert r.status_code == 200
    assert r.json().get("mode") == "stats"

    # POST /api/v1/execute
    r = client.post(
        "/api/v1/execute",
        json={
            "query": "select 1",
            "organization": "org1",
            "super_name": "super_a",
            "user_hash": "u1",
            "engine": "DUCKDB",
            "with_scan": False,
            "preview_rows": 10,
        },
        headers=headers,
    )
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is True
    assert body.get("rows_preview_count") == 2


    # POST /api/v1/write (multipart)
    r = client.post(
        "/api/v1/write",
        data={
            "organization": "org1",
            "super_name": "super_a",
            "user_hash": "u1",
            "simple_name": "table_a",
            "overwrite_columns_csv": "day",
        },
        files={"data_file": ("data.arrow", b"fake", "application/octet-stream")},
        headers=headers,
    )
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is True
    assert body.get("inserted") == 5

    # POST /api/v1/stage/upload (multipart)
    r = client.post(
        "/api/v1/stage/upload",
        data={
            "organization": "org1",
            "super_name": "super_a",
            "staging_name": "stage1",
            "base_file_name": "dummy_file_01",
        },
        files={"data_file": ("data.arrow", b"fake", "application/octet-stream")},
        headers=headers,
    )
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is True
    assert body.get("saved_file_name") == "dummy_file_01_123.parquet"
