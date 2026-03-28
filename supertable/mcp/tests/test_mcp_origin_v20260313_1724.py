import pathlib

_SRC_DIR = pathlib.Path(__file__).resolve().parent.parent


def test_web_server_enables_proxy_headers():
    text = (_SRC_DIR / "web_server.py").read_text()
    assert "proxy_headers=" in text
    assert "forwarded_allow_ips=" in text
