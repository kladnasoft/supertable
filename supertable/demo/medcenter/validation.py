"""Validation shared by medcenter application boundaries."""

from __future__ import annotations

import re


_CANONICAL_MONTH_RE = re.compile(r"[0-9]{4}-(?:0[1-9]|1[0-2])\Z")
_INVALID_MONTH_MESSAGE = (
    "month must use canonical YYYY-MM with a month from 01 through 12"
)


def require_canonical_month(value: object) -> str:
    """Return *value* only when it is a strict, path-safe calendar month.

    ASCII digits are intentional.  Besides keeping filenames portable, this
    makes the returned value safe for the demo's fixed SQL string literals.
    The controlled error never reflects attacker-provided input.
    """
    if type(value) is not str or _CANONICAL_MONTH_RE.fullmatch(value) is None:
        raise ValueError(_INVALID_MONTH_MESSAGE)
    return value
