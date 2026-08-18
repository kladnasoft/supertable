"""Conservative validation for Redis-cached snapshot documents.

The Redis leaf payload is only a performance cache of the immutable snapshot
JSON selected by ``leaf.path``.  A partial payload must therefore miss the
cache and load that JSON; treating an omitted deletion-vector field as an
explicit empty state can resurrect physically retained deleted rows.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple


_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_MAX_LUA_EXACT_INTEGER = 2**53 - 1


def collect_share_row_filters(*documents: object) -> Tuple[str, ...]:
    """Return every distinct linked-share predicate in supported wrappers.

    Policy overlays have historically lived on the leaf itself, beside or
    inside ``payload``/``data``/``snapshot``, and in one nested ``snapshot``
    wrapper.  Traverse exactly those locations rather than recursively walking
    arbitrary user metadata (a schema may legitimately contain a column named
    ``_row_filter``).  The established catalog representation treats an
    absent marker, JSON null, and a blank string as unrestricted.  Any other
    non-string marker is authorization corruption and therefore fails closed.
    """
    candidates = []
    for document in documents:
        if not isinstance(document, dict):
            continue
        candidates.append(document)
        for wrapper_name in ("payload", "data", "snapshot"):
            wrapped = document.get(wrapper_name)
            if not isinstance(wrapped, dict):
                continue
            candidates.append(wrapped)
            nested = wrapped.get("snapshot")
            if isinstance(nested, dict):
                candidates.append(nested)

    filters = []
    for candidate in candidates:
        if "_row_filter" not in candidate:
            continue
        raw_filter = candidate.get("_row_filter")
        if raw_filter is None:
            continue
        if not isinstance(raw_filter, str):
            raise RuntimeError("Linked-share policy metadata is invalid")
        if not raw_filter.strip():
            continue
        if raw_filter not in filters:
            filters.append(raw_filter)
    return tuple(filters)


def combined_share_row_filter(*documents: object) -> Optional[str]:
    """Return the fail-closed conjunction of supported policy overlays."""
    filters = collect_share_row_filters(*documents)
    if not filters:
        return None
    if len(filters) == 1:
        return filters[0]
    return " AND ".join(f"({row_filter})" for row_filter in filters)


def snapshot_cache_payload(payload: object) -> Dict[str, Any]:
    """Return a cache document with an explicit linked-share policy state.

    Current publishers use ``None`` for an unrestricted snapshot.  A shallow
    copy preserves any direct or nested share overlay while ensuring readers
    can distinguish a current complete cache from a legacy cache that may have
    omitted authorization metadata.
    """
    if not isinstance(payload, dict):
        raise TypeError("Snapshot cache payload must be a JSON object")
    normalized = dict(payload)
    normalized.setdefault("_row_filter", None)
    # Validate every supported wrapper marker before publishing it.  The SQL
    # predicate itself is parsed at the protected read-view boundary.
    collect_share_row_filters(normalized)
    return normalized


def complete_snapshot_payload(
        payload: object,
        *,
        expected_version: object = None,
        require_policy_marker: bool = False,
) -> Optional[Dict[str, Any]]:
    """Return a complete cached snapshot, otherwise ``None``.

    Completeness includes an explicit deletion-vector state.  The only valid
    pointerless state is exactly ``(None, 0, None)``; an active pointer needs a
    positive exact integer count and the canonical lowercase SHA-256 seal.
    Invalid or legacy payloads deliberately fall back to the heavy immutable
    JSON rather than guessing that no tombstone exists.  Redis callers must
    set ``require_policy_marker=True``: only an explicit ``_row_filter`` key
    proves that the cache publisher represented the linked-share policy state.
    Immutable snapshot objects may omit that key; absence there is the
    established unrestricted representation.
    """
    if not isinstance(payload, dict):
        return None

    if require_policy_marker and "_row_filter" not in payload:
        return None

    candidate = payload
    nested = payload.get("snapshot")
    if not isinstance(candidate.get("resources"), list) and isinstance(
            nested, dict,
    ):
        candidate = nested

    required = {
        "snapshot_version", "schema", "resources", "tombstone",
        "tombstone_rows", "tombstone_digest",
    }
    if not required.issubset(candidate):
        return None
    if not isinstance(candidate.get("resources"), list):
        return None
    if not isinstance(candidate.get("schema"), (dict, list)):
        return None

    version = candidate.get("snapshot_version")
    if not (
        isinstance(version, int)
        and not isinstance(version, bool)
        and 0 <= version <= _MAX_LUA_EXACT_INTEGER
    ):
        return None
    if expected_version is not None:
        if not (
            isinstance(expected_version, int)
            and not isinstance(expected_version, bool)
            and 0 <= expected_version <= _MAX_LUA_EXACT_INTEGER
            and expected_version == version
        ):
            return None

    pointer = candidate.get("tombstone")
    count = candidate.get("tombstone_rows")
    digest = candidate.get("tombstone_digest")
    exact_count = isinstance(count, int) and not isinstance(count, bool)
    if pointer is None:
        if not (exact_count and count == 0 and digest is None):
            return None
    elif isinstance(pointer, str) and bool(pointer):
        if not (
            exact_count
            and count > 0
            and isinstance(digest, str)
            and _SHA256_RE.fullmatch(digest)
        ):
            return None
    else:
        return None

    return candidate
