# route: supertable.audit.diagnostics
"""Bounded diagnostic metadata shared by audit boundaries."""
from __future__ import annotations

from supertable.utils.diagnostic_redaction import safe_exception_type


def safe_audit_error_type(error: BaseException) -> str:
    """Return a published class label without rendering exception content."""

    return safe_exception_type(error)


__all__ = ["safe_audit_error_type"]
