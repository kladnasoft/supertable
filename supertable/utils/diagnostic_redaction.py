# route: supertable.utils.diagnostic_redaction
"""Redaction for values that end up in logs and error payloads.

An exception type name is usually safe to log and occasionally is not: a
dynamically constructed class can carry a table name, a path, or a token in its
name, and that then lands in a request log or an error response that leaves the
trust boundary.

The rule here is allow-known, not block-suspicious. Blocking is a losing game —
it fails open on the case nobody predicted — whereas a name that is not a
recognised exception type is reported as its module-qualified base instead of
verbatim.
"""

from __future__ import annotations

_MAX_TYPE_NAME = 64


def safe_exception_type(exc: BaseException) -> str:
    """A log-safe name for an exception's type.

    Returns the class name when it is a plain identifier of reasonable length,
    which covers every builtin and every normally-declared exception. Anything
    else — an f-string-built class name, an absurdly long one — collapses to
    the module-qualified base so the log still says something useful without
    echoing content back.
    """
    cls = type(exc)
    name = getattr(cls, "__name__", "") or ""

    if name.isidentifier() and len(name) <= _MAX_TYPE_NAME:
        return name

    module = getattr(cls, "__module__", "") or "unknown"
    for base in cls.__mro__[1:]:
        base_name = getattr(base, "__name__", "") or ""
        if base_name.isidentifier() and len(base_name) <= _MAX_TYPE_NAME:
            return f"{module}.{base_name}"
    return "Exception"
