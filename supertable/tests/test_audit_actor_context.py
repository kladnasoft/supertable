from supertable import data_reader


def test_data_reader_accepts_external_audit_actor_context(monkeypatch) -> None:
    monkeypatch.setattr(data_reader, "get_storage", lambda: object())
    reader = data_reader.DataReader(
        "lake",
        "acme",
        "SELECT 1",
        audit_actor_id="user-1",
        audit_actor_username="alice",
    )
    assert reader.audit_actor_id == "user-1"
    assert reader.audit_actor_username == "alice"
