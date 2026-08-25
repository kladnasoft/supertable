from unittest.mock import MagicMock

import pyarrow as pa
import pytest

import supertable.data_reader as data_reader
from supertable.data_reader import _redact_storage_credentials
from supertable.engine.data_estimator import _safe_path_for_log
from supertable.engine.engine_common import redact_url_credentials
from supertable.processing import _safe_storage_path_for_log


def test_presigned_storage_credentials_are_redacted_from_user_errors():
    secret = "https://store/bucket/dv.parquet?X-Amz-Signature=SECRET&X-Amz-Expires=60"
    message = _redact_storage_credentials(f"HTTP GET failed for '{secret}'")

    assert "SECRET" not in message
    assert "X-Amz" not in message
    assert "https://store/<redacted-path>" in message
    assert "bucket" not in message
    assert "dv.parquet" not in message


def test_estimator_debug_path_redacts_sas_query():
    secret = "https://acct.blob.core.windows.net/c/dv.parquet?sig=SECRET&sp=r"
    rendered = _safe_path_for_log(secret)

    assert "SECRET" not in rendered
    assert rendered == "https://acct.blob.core.windows.net/<redacted-path>"
    assert "dv.parquet" not in rendered


def test_estimator_debug_path_redacts_userinfo_for_non_http_uri():
    rendered = _safe_path_for_log(
        "s3://access-key:secret-key@bucket.invalid/private/data.parquet"
        "?credential=SECRET#fragment"
    )

    assert rendered == "s3://bucket.invalid/<redacted-path>"
    assert "private" not in rendered
    assert "data.parquet" not in rendered


def test_engine_retry_log_redacts_credentials_embedded_in_error_text():
    error = RuntimeError(
        "HTTP GET https://store/b/dv?X-Amz-Signature=SECRET failed (403)"
    )
    rendered = redact_url_credentials(error)

    assert "SECRET" not in rendered
    assert "X-Amz" not in rendered
    assert "https://store/<redacted-path>" in rendered
    assert "/b/dv" not in rendered


def test_processing_diagnostics_redact_unsigned_remote_paths_too() -> None:
    rendered = _safe_storage_path_for_log(
        "abfss://container@account.invalid/TENANT_PATH_TOKEN/data.parquet"
    )

    assert rendered == "abfss://account.invalid/<redacted-path>"
    assert "TENANT_PATH_TOKEN" not in rendered
    assert "data.parquet" not in rendered


def test_local_storage_diagnostics_retain_only_path_metadata() -> None:
    local = "/var/lib/supertable/local-table/data.parquet"

    estimator_rendered = _safe_path_for_log(local)
    processing_rendered = _safe_storage_path_for_log(local)
    assert local not in estimator_rendered
    assert local not in processing_rendered
    assert estimator_rendered.startswith("path_bytes=")
    assert processing_rendered.startswith("path_bytes=")


def test_public_read_redactor_removes_every_remote_url_component() -> None:
    message = _redact_storage_credentials(
        "failed s3://USER:PASSWORD@bucket.invalid/PATH_TOKEN/file.parquet"
        "?QUERY_TOKEN=yes#FRAGMENT_TOKEN"
    )

    assert message == "failed s3://bucket.invalid/<redacted-path>"
    for secret in (
        "USER", "PASSWORD", "PATH_TOKEN", "file.parquet",
        "QUERY_TOKEN", "FRAGMENT_TOKEN",
    ):
        assert secret not in message


@pytest.mark.parametrize(
    "diagnostic, secret",
    [
        ("Authorization: Bearer READ_AUTH_SECRET", "READ_AUTH_SECRET"),
        ("Cookie: session=READ_COOKIE_SECRET", "READ_COOKIE_SECRET"),
        ("X-Api-Key: READ_API_SECRET", "READ_API_SECRET"),
        ('{"access_token":"READ_BODY_SECRET"}', "READ_BODY_SECRET"),
    ],
)
def test_shared_public_redactor_scrubs_header_and_body_credentials(
    diagnostic, secret,
) -> None:
    rendered = redact_url_credentials(diagnostic)

    assert secret not in rendered
    assert "<redacted>" in rendered


def test_parser_phase_never_returns_or_logs_sql_literals_or_backend_urls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = (
        "SELECT 'patient-secret' FROM private; "
        "https://store/object?X-Amz-Signature=URL-SECRET"
    )
    monkeypatch.setattr(data_reader, "get_storage", MagicMock())
    monkeypatch.setattr(
        data_reader,
        "classify_query",
        MagicMock(side_effect=ValueError(secret)),
    )
    warning = MagicMock()
    monkeypatch.setattr(data_reader.logger, "warning", warning)

    frame, status, message = data_reader.DataReader(
        "medical", "acme", secret,
    ).execute("reader")

    assert frame.empty
    assert status is data_reader.Status.ERROR
    assert message == "Query is invalid or unsupported"
    rendered = repr(warning.call_args_list) + str(message)
    assert "patient-secret" not in rendered
    assert "URL-SECRET" not in rendered
    assert "X-Amz" not in rendered
    assert "ValueError" in rendered


def test_stream_failure_uses_fixed_public_and_monitoring_message() -> None:
    secret = (
        "SELECT 'stream-secret'; "
        "https://store/object?sig=PRESIGNED-SECRET"
    )

    class FailingStream:
        schema = pa.schema([("id", pa.int64())])

        def __iter__(self):
            return self

        def __next__(self):
            raise RuntimeError(secret)

    outcomes: list[tuple[object, ...]] = []
    stream = data_reader._MonitoredResultStream(
        FailingStream(), lambda *args: outcomes.append(args),
    )

    with pytest.raises(RuntimeError, match="Query result stream failed") as raised:
        next(stream)

    assert secret not in str(raised.value)
    assert outcomes == [
        ("error", "Query result stream failed", 0, 0),
    ]
    assert "stream-secret" not in repr(outcomes)
    assert "PRESIGNED-SECRET" not in repr(outcomes)
