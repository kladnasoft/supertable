from types import SimpleNamespace

import pytest

pytest.importorskip("google.cloud.storage")

from supertable.storage import gcp_storage


def test_missing_explicit_gcs_credentials_fail_closed(monkeypatch) -> None:
    monkeypatch.setattr(
        gcp_storage,
        "settings",
        SimpleNamespace(
            effective_gcs_bucket="bucket",
            SUPERTABLE_PREFIX="",
            GOOGLE_APPLICATION_CREDENTIALS="/definitely/missing/credentials.json",
            GCP_SA_JSON="",
            GCP_PROJECT="",
        ),
    )
    with pytest.raises(FileNotFoundError, match="credentials"):
        gcp_storage.GCSStorage.from_env()
