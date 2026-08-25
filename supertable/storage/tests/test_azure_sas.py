from types import SimpleNamespace

from supertable.storage import azure_storage


def test_sas_credential_is_kept_out_of_account_url(monkeypatch) -> None:
    captured = {}

    class FakeService:
        account_name = "acct"
        url = "https://acct.blob.core.windows.net"
        credential = "sv=1&sig=secret"

        def get_container_client(self, _container):
            return object()

    def fake_client(**kwargs):
        captured.update(kwargs)
        return FakeService()

    monkeypatch.setattr(azure_storage, "BlobServiceClient", fake_client)
    monkeypatch.setattr(
        azure_storage,
        "settings",
        SimpleNamespace(
            SUPERTABLE_HOME="",
            AZURE_STORAGE_ACCOUNT="acct",
            effective_storage_bucket="container",
            effective_storage_endpoint="https://acct.blob.core.windows.net",
            SUPERTABLE_PREFIX="",
            AZURE_STORAGE_CONNECTION_STRING="",
            effective_storage_access_key="",
            AZURE_SAS_TOKEN="?sv=1&sig=secret",
        ),
    )
    azure_storage.AzureBlobStorage.from_env()
    assert captured["account_url"] == "https://acct.blob.core.windows.net"
    assert captured["credential"] == "sv=1&sig=secret"
