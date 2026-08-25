import importlib


def test_redis_infra_import_does_not_construct_redis(monkeypatch):
    import supertable.redis_infra as infra

    called = []
    monkeypatch.setattr(infra, "_build_redis_client", lambda: called.append(True))
    importlib.reload(infra)
    assert called == []
