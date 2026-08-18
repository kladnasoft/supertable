"""Pure validation helpers for Spark cluster storage configuration.

This module intentionally imports only the standard library so the Redis
catalog can validate cluster registrations without importing a query engine.
"""

from __future__ import annotations

import re
from typing import Mapping
from urllib.parse import urlparse


INLINE_SPARK_STORAGE_CREDENTIAL_KEYS = (
    "s3_access_key",
    "s3_secret_key",
    "s3_session_token",
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
)

_OBJECT_STORAGE_CREDENTIAL_SUFFIXES = (
    "access_key",
    "access_key_id",
    "secret_key",
    "secret_access_key",
    "session_token",
    "security_token",
    "sas_token",
    "account_key",
    "client_secret",
    "service_account_key",
    "private_key",
    "credentials_json",
    "application_credentials",
    "credentials_provider",
    "json_keyfile",
    "keyfile",
)
_OBJECT_STORAGE_CREDENTIAL_MARKERS = (
    "fs_azure_account_key_",
    "fs_azure_sas_",
    "client_secret_",
    "private_key_",
)

_REGION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_DNS_OR_IPV6_RE = re.compile(r"^(?:[A-Za-z0-9.-]+|[0-9A-Fa-f:]+)$")


def _endpoint(value: object) -> str:
    if not isinstance(value, str) or not value or len(value) > 512:
        raise ValueError("Spark s3_endpoint must be a valid HTTP(S) endpoint")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
        raise ValueError("Spark s3_endpoint contains a control character")
    try:
        parsed = urlparse(value)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Spark s3_endpoint must be a valid HTTP(S) endpoint") from exc
    if (
        parsed.scheme.casefold() not in {"http", "https"}
        or not parsed.hostname
        or not _DNS_OR_IPV6_RE.fullmatch(parsed.hostname)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise ValueError("Spark s3_endpoint must be a valid HTTP(S) endpoint")
    return value.rstrip("/")


def _region(value: object) -> str:
    if not isinstance(value, str) or not _REGION_RE.fullmatch(value):
        raise ValueError("Spark s3_region has an invalid format")
    return value


def _boolean(name: str, value: object) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, str) and value.casefold() in {"true", "false"}:
        return value.casefold()
    raise ValueError(f"Spark {name} must be a boolean")


def spark_storage_credential_keys(config: Mapping[str, object] | None) -> tuple[str, ...]:
    """Return non-empty object-store credential fields, case/separator agnostic.

    ``password`` is intentionally absent: it authenticates the HiveServer2
    transport and is not installed into Spark/Hadoop object-store settings.
    """
    found = []
    for raw_key, value in (config or {}).items():
        if value in (None, ""):
            continue
        key = re.sub(r"[^a-z0-9]+", "_", str(raw_key).casefold()).strip("_")
        compact_key = key.replace("_", "")
        if key in INLINE_SPARK_STORAGE_CREDENTIAL_KEYS or any(
            key == suffix or key.endswith("_" + suffix)
            for suffix in _OBJECT_STORAGE_CREDENTIAL_SUFFIXES
        ) or any(
            compact_key == suffix.replace("_", "")
            or compact_key.endswith(suffix.replace("_", ""))
            for suffix in _OBJECT_STORAGE_CREDENTIAL_SUFFIXES
        ) or any(marker in key for marker in _OBJECT_STORAGE_CREDENTIAL_MARKERS):
            found.append(str(raw_key))
    return tuple(found)


def validate_spark_storage_config(config: Mapping[str, object] | None) -> dict[str, str]:
    """Reject inline credentials/injection and normalize non-secret overrides."""
    values = config or {}
    if spark_storage_credential_keys(values):
        raise ValueError(
            "Inline Spark object-store credentials are disabled; configure "
            "cluster-side workload identity or a Hadoop credential provider"
        )

    normalized: dict[str, str] = {}
    if values.get("s3_endpoint") not in (None, ""):
        normalized["s3_endpoint"] = _endpoint(values["s3_endpoint"])
    if values.get("s3_region") not in (None, ""):
        normalized["s3_region"] = _region(values["s3_region"])
    if values.get("s3_use_ssl") is not None:
        normalized["s3_use_ssl"] = _boolean(
            "s3_use_ssl", values["s3_use_ssl"]
        )
    if values.get("s3_path_style") is not None:
        normalized["s3_path_style"] = _boolean(
            "s3_path_style", values["s3_path_style"]
        )
    return normalized


__all__ = [
    "INLINE_SPARK_STORAGE_CREDENTIAL_KEYS",
    "spark_storage_credential_keys",
    "validate_spark_storage_config",
]
