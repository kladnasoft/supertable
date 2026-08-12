"""Conservative validation for Redis-cached snapshot documents.

The Redis leaf payload is only a performance cache of the immutable snapshot
JSON selected by ``leaf.path``.  A partial payload must therefore miss the
cache and load that JSON; treating an omitted deletion-vector field as an
explicit empty state can resurrect physically retained deleted rows.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional


_SHA256_RE = re.compile(r"[0-9a-f]{64}")


def complete_snapshot_payload(
        payload: object,
        *,
        expected_version: object = None,
) -> Optional[Dict[str, Any]]:
    """Return a complete cached snapshot, otherwise ``None``.

    Completeness includes an explicit deletion-vector state.  The only valid
    pointerless state is exactly ``(None, 0, None)``; an active pointer needs a
    positive exact integer count and the canonical lowercase SHA-256 seal.
    Invalid or legacy payloads deliberately fall back to the heavy immutable
    JSON rather than guessing that no tombstone exists.
    """
    if not isinstance(payload, dict):
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
        and version >= 0
    ):
        return None
    if expected_version is not None:
        if not (
            isinstance(expected_version, int)
            and not isinstance(expected_version, bool)
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
