"""Confidential, bounded metadata for mirror-publication failures.

Backend exception messages are not control-plane metadata.  Storage SDKs may
embed signed URLs, credentials, request headers, or provider response bodies in
``str(exc)``.  Mirror durability records, logs, and public exception strings
therefore retain only a conservative exception type and a controlled stage.
The original exception object can still be chained or attached in memory.
"""

from __future__ import annotations

import re

from supertable.utils.diagnostic_redaction import safe_exception_type


_ERROR_TYPE_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z", re.ASCII)
_MIRROR_FAILURE_STAGES = frozenset(
    {
        "core_commit",
        "mirror",
        "outbox_complete",
        "recovery:core_not_committed",
        "recovery:mirror",
        "mirror:DELTA",
        "mirror:ICEBERG",
        "mirror:PARQUET",
    }
)


def normalize_mirror_error_type(value: object) -> str:
    """Return one bounded, inert exception-type label."""

    if isinstance(value, str) and _ERROR_TYPE_RE.fullmatch(value):
        return value
    return "Exception"


def mirror_error_type(error: BaseException) -> str:
    """Extract safe diagnostic metadata without formatting ``error``."""

    return safe_exception_type(error)


def normalize_mirror_failure_stage(value: object) -> str:
    """Accept only stages emitted by the mirror publication state machine."""

    if not isinstance(value, str) or value not in _MIRROR_FAILURE_STAGES:
        raise ValueError("Invalid mirror publication failure stage")
    return value


__all__ = [
    "mirror_error_type",
    "normalize_mirror_error_type",
    "normalize_mirror_failure_stage",
]
