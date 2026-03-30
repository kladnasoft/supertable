# route: supertable.services.error_codes
"""
Structured error codes for the SuperTable API.

Every API error response includes a machine-readable ``code`` field
alongside the human-readable ``detail``.  Clients can switch on the
code for programmatic error handling without string-matching messages.

Usage in endpoint handlers:

    from supertable.services.error_codes import api_error, E

    raise api_error(E.MISSING_ORG_SUP)
    raise api_error(E.TABLE_NOT_FOUND, table="orders")
    raise api_error(E.WRITE_FAILED, detail="disk full")

The error response body is:

    {
        "detail": "Organization and SuperTable are required",
        "code": "MISSING_ORG_SUP",
        "status": 400
    }
"""
from __future__ import annotations

from fastapi import HTTPException
from fastapi.responses import JSONResponse


class E:
    """Error code constants grouped by category.

    Naming convention: CATEGORY_VERB or CATEGORY_NOUN.
    """

    # ── Auth (401 / 403) ───────────────────────────────────
    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"
    SUPERUSER_REQUIRED = "SUPERUSER_REQUIRED"
    INVALID_TOKEN = "INVALID_TOKEN"
    TOKEN_MISMATCH = "TOKEN_MISMATCH"

    # ── Validation (400) ───────────────────────────────────
    MISSING_ORG_SUP = "MISSING_ORG_SUP"
    MISSING_PARAMETER = "MISSING_PARAMETER"
    INVALID_PARAMETER = "INVALID_PARAMETER"
    INVALID_USERNAME = "INVALID_USERNAME"
    INVALID_TABLE_NAME = "INVALID_TABLE_NAME"
    INVALID_STAGING_NAME = "INVALID_STAGING_NAME"
    INVALID_PIPE_NAME = "INVALID_PIPE_NAME"
    INVALID_FILE_TYPE = "INVALID_FILE_TYPE"
    INVALID_SQL = "INVALID_SQL"
    RESERVED_NAME = "RESERVED_NAME"

    # ── Not found (404) ───────────────────────────────────
    TABLE_NOT_FOUND = "TABLE_NOT_FOUND"
    ROLE_NOT_FOUND = "ROLE_NOT_FOUND"
    USER_NOT_FOUND = "USER_NOT_FOUND"
    TOKEN_NOT_FOUND = "TOKEN_NOT_FOUND"
    STAGING_NOT_FOUND = "STAGING_NOT_FOUND"
    PIPE_NOT_FOUND = "PIPE_NOT_FOUND"
    ENDPOINT_NOT_FOUND = "ENDPOINT_NOT_FOUND"
    SNAPSHOT_NOT_FOUND = "SNAPSHOT_NOT_FOUND"
    SUPERTABLE_NOT_FOUND = "SUPERTABLE_NOT_FOUND"

    # ── Conflict (409) ────────────────────────────────────
    DUPLICATE_USERNAME = "DUPLICATE_USERNAME"
    DUPLICATE_ROLE_NAME = "DUPLICATE_ROLE_NAME"
    DUPLICATE_STAGING = "DUPLICATE_STAGING"

    # ── Protected (403) ───────────────────────────────────
    PROTECTED_ACCOUNT = "PROTECTED_ACCOUNT"
    PROTECTED_ROLE = "PROTECTED_ROLE"

    # ── Server / operation errors (500) ───────────────────
    WRITE_FAILED = "WRITE_FAILED"
    DELETE_FAILED = "DELETE_FAILED"
    QUERY_FAILED = "QUERY_FAILED"
    IMPORT_FAILED = "IMPORT_FAILED"
    STORAGE_ERROR = "STORAGE_ERROR"
    REDIS_ERROR = "REDIS_ERROR"
    LOCK_TIMEOUT = "LOCK_TIMEOUT"
    INTERNAL_ERROR = "INTERNAL_ERROR"

    # ── Health (503) ──────────────────────────────────────
    SERVICE_DEGRADED = "SERVICE_DEGRADED"


# Default HTTP status code and message per error code
_DEFAULTS = {
    E.UNAUTHORIZED:        (401, "Unauthorized"),
    E.FORBIDDEN:           (403, "Forbidden"),
    E.SUPERUSER_REQUIRED:  (403, "This operation requires superuser access"),
    E.INVALID_TOKEN:       (401, "Invalid or expired token"),
    E.TOKEN_MISMATCH:      (401, "Token does not match username"),
    E.MISSING_ORG_SUP:     (400, "Organization and SuperTable are required"),
    E.MISSING_PARAMETER:   (400, "Required parameter is missing"),
    E.INVALID_PARAMETER:   (400, "Invalid parameter value"),
    E.INVALID_USERNAME:    (400, "Invalid username format"),
    E.INVALID_TABLE_NAME:  (400, "Invalid table name"),
    E.INVALID_STAGING_NAME:(400, "Invalid staging name"),
    E.INVALID_PIPE_NAME:   (400, "Invalid pipe name"),
    E.INVALID_FILE_TYPE:   (400, "Unsupported file type"),
    E.INVALID_SQL:         (400, "Only SELECT or WITH (CTE) queries are allowed"),
    E.RESERVED_NAME:       (403, "Reserved system name cannot be used"),
    E.TABLE_NOT_FOUND:     (404, "Table not found"),
    E.ROLE_NOT_FOUND:      (404, "Role not found"),
    E.USER_NOT_FOUND:      (404, "User not found"),
    E.TOKEN_NOT_FOUND:     (404, "Token not found"),
    E.STAGING_NOT_FOUND:   (404, "Staging area not found"),
    E.PIPE_NOT_FOUND:      (404, "Pipe not found"),
    E.ENDPOINT_NOT_FOUND:  (404, "OData endpoint not found"),
    E.SNAPSHOT_NOT_FOUND:  (404, "Snapshot version not found"),
    E.SUPERTABLE_NOT_FOUND:(404, "SuperTable not found"),
    E.DUPLICATE_USERNAME:  (409, "Username already exists"),
    E.DUPLICATE_ROLE_NAME: (409, "Role name already exists"),
    E.DUPLICATE_STAGING:   (409, "Staging area already exists"),
    E.PROTECTED_ACCOUNT:   (403, "System accounts cannot be modified or deleted"),
    E.PROTECTED_ROLE:      (403, "This role type cannot be assigned from the UI"),
    E.WRITE_FAILED:        (500, "Data write failed"),
    E.DELETE_FAILED:        (500, "Delete operation failed"),
    E.QUERY_FAILED:        (500, "Query execution failed"),
    E.IMPORT_FAILED:       (500, "Module import failed"),
    E.STORAGE_ERROR:       (500, "Storage backend error"),
    E.REDIS_ERROR:         (500, "Redis operation failed"),
    E.LOCK_TIMEOUT:        (500, "Could not acquire lock — another operation may be in progress"),
    E.INTERNAL_ERROR:      (500, "Internal server error"),
    E.SERVICE_DEGRADED:    (503, "Service is degraded"),
}


def api_error(
    code: str,
    *,
    detail: str = "",
    status: int = 0,
    **extra,
) -> HTTPException:
    """Create an HTTPException with a structured error code.

    The response body will be:
        {"detail": "...", "code": "...", "status": N, ...extra}

    Args:
        code: One of the ``E.*`` constants.
        detail: Override the default message. If empty, uses the default.
        status: Override the default HTTP status. If 0, uses the default.
        **extra: Additional fields merged into the response body
                 (e.g. ``table="orders"``, ``field="role_name"``).
    """
    default_status, default_detail = _DEFAULTS.get(code, (500, "Unknown error"))
    status_code = status or default_status
    message = detail or default_detail

    body = {"detail": message, "code": code, "status": status_code}
    body.update(extra)

    return HTTPException(status_code=status_code, detail=body)
