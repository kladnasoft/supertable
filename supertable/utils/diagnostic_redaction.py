"""Small, dependency-free helpers for confidentiality-safe diagnostics.

Remote object paths commonly contain tenant identifiers, opaque access
tokens, or customer-selected object names even when the URL has no query
string.  Diagnostics may therefore retain only the remote authority; the
entire path/query/fragment portion is represented by a fixed marker.
"""

from __future__ import annotations

import hashlib
import re
import sys
from types import ModuleType
from urllib.parse import urlsplit, urlunsplit


_REMOTE_URL_RE = re.compile(
    r"(?i)\b[a-z][a-z0-9+.-]{0,31}://[^\s'\"<>]+"
)
_REMOTE_URL_PREFIX_RE = re.compile(
    r"(?i)^[a-z][a-z0-9+.-]{0,31}://"
)
_TRAILING_URL_PUNCTUATION = ").,;]}"
_MAX_SAFE_HOST_BYTES = 512
_AUTH_OR_COOKIE_HEADER_RE = re.compile(
    r"(?im)\b(authorization|proxy-authorization|cookie|set-cookie)"
    r"\s*:\s*[^\r\n]*"
)
_API_KEY_HEADER_RE = re.compile(
    r"(?i)\b(x-api-key|api-key)\s*:\s*"
    r"(?:\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|[^\s,;\]}]+)"
)
_JSON_SECRET_RE = re.compile(
    r"(?i)([\"']?(?:access_token|refresh_token|id_token|api_key|api-key|"
    r"password|secret|token)[\"']?\s*:\s*)"
    r"(?:\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|[^,\s\]}]+)"
)
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(access_token|refresh_token|id_token|api[_-]?key|password|"
    r"secret|token|signature|x-amz-(?:signature|credential|security-token))"
    r"(\s*=\s*)(?:\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|"
    r"[^&\s,;\]}]+)"
)
_AUTH_SCHEME_RE = re.compile(
    r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+"
)
_EXCEPTION_TYPE_RE = re.compile(
    r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z", re.ASCII,
)


def _published_type_name(value_type: type) -> str | None:
    """Return a bounded class name only when its module publishes that class.

    Exception classes may be created dynamically and their ``__name__`` is a
    writable attribute.  Merely applying an ASCII/length filter would still let
    attacker-selected identifier text reach logs and public diagnostics.  A
    normal, statically declared exception is published by its defining module
    under its own name; a request-created class generally is not.
    """

    try:
        name = type.__getattribute__(value_type, "__name__")
        module_name = type.__getattribute__(value_type, "__module__")
    except (AttributeError, TypeError):
        return None
    if (
        not isinstance(name, str)
        or not _EXCEPTION_TYPE_RE.fullmatch(name)
        or not isinstance(module_name, str)
    ):
        return None
    module = sys.modules.get(module_name)
    if not isinstance(module, ModuleType):
        return None
    try:
        published = vars(module).get(name)
    except TypeError:
        return None
    return name if published is value_type else None


def safe_exception_type(error: BaseException) -> str:
    """Return inert, bounded ASCII exception metadata without formatting it.

    Statically published exceptions retain their useful concrete name.  For a
    runtime-generated subclass, the nearest published built-in exception base
    preserves broad taxonomy (for example ``TimeoutError`` or ``ValueError``)
    without reflecting the dynamic class name.  The exception's message and
    ``__str__`` method are never inspected.
    """

    exception_type = type(error)
    if not issubclass(exception_type, BaseException):
        return "Exception"
    try:
        hierarchy = type.__getattribute__(exception_type, "__mro__")
    except (AttributeError, TypeError):
        return "Exception"
    for candidate in hierarchy:
        published_name = _published_type_name(candidate)
        if published_name is not None:
            return published_name
    return "Exception"


def safe_value_type(value: object) -> str:
    """Return a published type label without reflecting dynamic class names."""

    value_type = type(value)
    try:
        hierarchy = type.__getattribute__(value_type, "__mro__")
    except (AttributeError, TypeError):
        return "object"
    for candidate in hierarchy:
        published_name = _published_type_name(candidate)
        if published_name is not None:
            return published_name
    return "object"


def _remote_authority_only(url: str) -> str:
    """Return one remote URL with its authority only, or a fixed fallback."""

    try:
        parsed = urlsplit(url)
        if not parsed.scheme or not parsed.netloc or not parsed.hostname:
            return "<redacted-url>"
        host = parsed.hostname
        host_bytes = host.encode("utf-8", errors="strict")
        if (
            len(host_bytes) > _MAX_SAFE_HOST_BYTES
            or any(ord(char) < 0x20 or ord(char) == 0x7F for char in host)
        ):
            return "<redacted-url>"
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        port = parsed.port
        authority = host + (f":{port}" if port is not None else "")
        path_marker = (
            "/<redacted-path>"
            if parsed.path or parsed.query or parsed.fragment
            else ""
        )
        return urlunsplit((parsed.scheme, authority, path_marker, "", ""))
    except (UnicodeError, ValueError):
        return "<redacted-url>"


def redact_remote_urls(value: object) -> str:
    """Redact every syntactically URL-like remote path embedded in ``value``.

    User-info, path, query, and fragment bytes are never retained.  Trailing
    prose punctuation is preserved so ordinary error messages stay readable.
    """

    text = str(value or "")

    def replace(match: re.Match[str]) -> str:
        raw = match.group(0)
        url = raw.rstrip(_TRAILING_URL_PUNCTUATION)
        trailing = raw[len(url) :]
        return _remote_authority_only(url) + trailing

    return _REMOTE_URL_RE.sub(replace, text)


def redact_sensitive_diagnostic_text(value: object) -> str:
    """Redact remote URLs and common header/body credential renderings."""

    text = redact_remote_urls(value)
    text = _AUTH_OR_COOKIE_HEADER_RE.sub(
        lambda match: f"{match.group(1)}: <redacted>", text,
    )
    text = _API_KEY_HEADER_RE.sub(
        lambda match: f"{match.group(1)}: <redacted>", text,
    )
    text = _JSON_SECRET_RE.sub(
        lambda match: f'{match.group(1)}"<redacted>"', text,
    )
    text = _SECRET_ASSIGNMENT_RE.sub(
        lambda match: f"{match.group(1)}{match.group(2)}<redacted>", text,
    )
    return _AUTH_SCHEME_RE.sub(
        lambda match: f"{match.group(1)} <redacted>", text,
    )


def safe_storage_path_for_diagnostic(value: object) -> str:
    """Keep local paths but replace any remote path with authority metadata."""

    text = str(value or "")
    if not _REMOTE_URL_PREFIX_RE.match(text):
        return text
    return _remote_authority_only(text)


def local_path_metadata(value: object) -> str:
    """Return bounded correlation metadata without exposing a local path."""

    encoded = str(value or "").encode("utf-8", errors="surrogatepass")
    digest = hashlib.sha256(encoded).hexdigest()[:16]
    return f"path_bytes={len(encoded)}; path_sha256={digest}"


__all__ = [
    "local_path_metadata",
    "redact_remote_urls",
    "redact_sensitive_diagnostic_text",
    "safe_exception_type",
    "safe_storage_path_for_diagnostic",
    "safe_value_type",
]
