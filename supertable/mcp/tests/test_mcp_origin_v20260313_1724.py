import importlib.util
import pathlib


def test_caddyfile_does_not_rewrite_host_header():
    text = pathlib.Path("/mnt/data/Caddyfile.https_v20260313_1724_preserve-host").read_text()
    assert "header_up Host localhost:8099" not in text
    assert "reverse_proxy supertable-mcp:8099" in text
    assert "X-Forwarded-Proto" in text


def test_web_server_enables_proxy_headers():
    text = pathlib.Path("/mnt/data/web_server_v20260313_1724_proxy-headers.py").read_text()
    assert "proxy_headers=" in text
    assert "forwarded_allow_ips=" in text
