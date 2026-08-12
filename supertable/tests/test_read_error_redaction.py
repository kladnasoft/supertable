from supertable.data_reader import _redact_storage_credentials
from supertable.engine.data_estimator import _safe_path_for_log
from supertable.engine.engine_common import redact_url_credentials


def test_presigned_storage_credentials_are_redacted_from_user_errors():
    secret = "https://store/bucket/dv.parquet?X-Amz-Signature=SECRET&X-Amz-Expires=60"
    message = _redact_storage_credentials(f"HTTP GET failed for '{secret}'")

    assert "SECRET" not in message
    assert "X-Amz" not in message
    assert "https://store/bucket/dv.parquet?<redacted>" in message


def test_estimator_debug_path_redacts_sas_query():
    secret = "https://acct.blob.core.windows.net/c/dv.parquet?sig=SECRET&sp=r"
    rendered = _safe_path_for_log(secret)

    assert "SECRET" not in rendered
    assert rendered == "https://acct.blob.core.windows.net/c/dv.parquet?<redacted>"


def test_engine_retry_log_redacts_credentials_embedded_in_error_text():
    error = RuntimeError(
        "HTTP GET https://store/b/dv?X-Amz-Signature=SECRET failed (403)"
    )
    rendered = redact_url_credentials(error)

    assert "SECRET" not in rendered
    assert "X-Amz" not in rendered
    assert "https://store/b/dv?<redacted>" in rendered
