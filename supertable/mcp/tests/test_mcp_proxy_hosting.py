import importlib.util
import os
from pathlib import Path

def _load_module(path: str, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module

def test_web_server_enables_proxy_headers(monkeypatch):
    path = Path("/mnt/data/web_server_v20260313_1713_proxy-headers.py")
    module = _load_module(str(path), "test_web_server_mod")
    called = {}

    def fake_run(*args, **kwargs):
        called["args"] = args
        called["kwargs"] = kwargs

    monkeypatch.setattr(module.uvicorn, "run", fake_run)
    monkeypatch.setenv("FORWARDED_ALLOW_IPS", "*")
    monkeypatch.setenv("UVICORN_PROXY_HEADERS", "1")
    monkeypatch.setenv("SUPERTABLE_MCP_WEB_PORT", "8099")
    module.main()

    assert called["kwargs"]["proxy_headers"] is True
    assert called["kwargs"]["forwarded_allow_ips"] == "*"

def test_web_app_configures_trusted_hosts_from_env():
    text = Path("/mnt/data/web_app_v20260313_1713_fix-host-origin.py").read_text()
    assert "TrustedHostMiddleware" in text
    assert "SUPERTABLE_ALLOWED_HOSTS" in text
