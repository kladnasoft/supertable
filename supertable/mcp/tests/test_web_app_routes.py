from __future__ import annotations

import importlib
import os
import shutil
import sys
import textwrap
from pathlib import Path

from fastapi.testclient import TestClient


def test_web_app_legacy_mcp_mount_keeps_routes(tmp_path: Path, monkeypatch) -> None:
    pkg = tmp_path / "supertable" / "mcp"
    pkg.mkdir(parents=True)
    (tmp_path / "supertable" / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "__init__.py").write_text("# mcp package\n", encoding="utf-8")

    source_dir = Path(__file__).resolve().parent
    mcp_src_dir = source_dir.parent  # supertable/mcp/ (actual source, not tests/)

    shutil.copy(mcp_src_dir / "web_app.py", pkg / "web_app.py")
    shutil.copy(mcp_src_dir / "web_client.py", pkg / "web_client.py")
    shutil.copy(mcp_src_dir / "web_tester.html", pkg / "web_tester.html")
    shutil.copy(mcp_src_dir / "mcp_simulator.html", pkg / "mcp_simulator.html")

    (pkg / "mcp_server.py").write_text(
        textwrap.dedent(
            """
            from __future__ import annotations
            from contextlib import asynccontextmanager

            from fastapi import FastAPI
            from fastapi.responses import JSONResponse


            class _SessionManager:
                @asynccontextmanager
                async def run(self):
                    yield


            class FakeMCP:
                def __init__(self):
                    self.session_manager = _SessionManager()

                def streamable_http_app(self):
                    app = FastAPI()

                    @app.get("/mcp")
                    async def mcp_get():
                        return JSONResponse({"ok": True, "path": "/mcp"})

                    @app.post("/mcp")
                    async def mcp_post():
                        return JSONResponse({"ok": True, "path": "/mcp"})

                    return app


            mcp = FakeMCP()
            """
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("SUPERTABLE_MCP_WEB_DISABLE_SUBPROCESS", "1")
    monkeypatch.setenv("SUPERTABLE_SUPERTOKEN", "secret")

    sys.path.insert(0, str(tmp_path))
    try:
        for name in list(sys.modules):
            if name.startswith("supertable"):
                del sys.modules[name]

        mod = importlib.import_module("supertable.mcp.web_app")

        with TestClient(mod.app) as client:
            health = client.get("/health")
            assert health.status_code == 200
            assert health.json()["mcp_streamable_http"] == "mounted"

            assert client.get("/web?auth=secret").status_code == 200
            assert client.get("/?auth=secret").status_code == 200
            assert client.get("/simulator?auth=secret").status_code == 200
            assert client.get("/simulation?auth=secret").status_code == 200

            mcp_get = client.get("/mcp")
            assert mcp_get.status_code == 200
            assert mcp_get.json()["path"] == "/mcp"
    finally:
        sys.path.pop(0)
        for name in list(sys.modules):
            if name.startswith("supertable"):
                del sys.modules[name]
