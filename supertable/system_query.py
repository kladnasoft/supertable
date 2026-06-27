# route: supertable.system_query
"""Read-path command classification.

The read path is intentionally restricted: ``DataReader`` only resolves and runs
queries that read existing data.  Historically this was enforced implicitly —
anything that wasn't a ``SELECT`` failed somewhere downstream (``EXPLAIN`` /
``SHOW`` parse to a sqlglot ``Command`` with no tables, so ``SQLParser`` raised
"No tables found").

This module makes the *allowed* set explicit and adds two diagnostic commands on
top of plain ``SELECT``:

  * ``EXPLAIN [ANALYZE] <select>`` — run the SELECT through the normal pipeline
    (estimation, reflection, RBAC, dedup) but ask DuckDB for the execution plan
    instead of the rows.
  * ``SHOW STATS [super.]simple`` — return the raw contents of a table's latest
    column-statistics parquet artifact, unfiltered.

Everything else falls through to :data:`CommandKind.SELECT` and is handled by the
existing path unchanged, so this classifier never tightens or loosens the
behaviour of ordinary queries — it only *adds* the two new prefixes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class CommandKind(Enum):
    SELECT = "select"
    EXPLAIN = "explain"
    SHOW_STATS = "show_stats"


@dataclass(frozen=True)
class SystemCommand:
    """A classified read-path command.

    - ``sql`` is the statement to feed to ``SQLParser`` / the executor.  For
      ``SELECT`` it is the raw query unchanged; for ``EXPLAIN`` it is the inner
      SELECT (the ``EXPLAIN`` prefix is re-applied by the executor).  Empty for
      ``SHOW STATS``.
    - ``explain`` / ``explain_options`` describe an ``EXPLAIN`` wrapper
      (``explain_options`` is e.g. ``"ANALYZE"`` or ``""``).
    - ``super_name`` / ``simple_name`` name the ``SHOW STATS`` target.
    """
    kind: CommandKind
    sql: str = ""
    explain: bool = False
    explain_options: str = ""
    super_name: Optional[str] = None
    simple_name: Optional[str] = None


# ``SHOW STATS <body>`` — body parsed separately for the table reference.
_SHOW_STATS_RE = re.compile(r"^\s*SHOW\s+STATS\b(?P<body>.*)$", re.IGNORECASE | re.DOTALL)

# ``EXPLAIN [ANALYZE] <inner>`` — opts captures an optional ANALYZE.
_EXPLAIN_RE = re.compile(
    r"^\s*EXPLAIN\s+(?P<opts>ANALYZE\s+)?(?P<inner>.+)$",
    re.IGNORECASE | re.DOTALL,
)

# The inner statement of an EXPLAIN must be a SELECT/WITH (read-only).
_SELECT_INNER_RE = re.compile(r"^\s*(?:WITH|SELECT)\b", re.IGNORECASE)

# A (possibly schema-qualified) identifier: bare, double-quoted, or back-ticked.
_IDENT = r'(?:"[^"]+"|`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*)'
_TABLE_REF_RE = re.compile(
    rf'^\s*(?:(?P<super>{_IDENT})\s*\.\s*)?(?P<simple>{_IDENT})\s*$'
)


def _unquote(ident: str) -> str:
    ident = ident.strip()
    if len(ident) >= 2 and ident[0] in '"`' and ident[-1] == ident[0]:
        return ident[1:-1]
    return ident


def classify_query(query: str, default_super: str) -> SystemCommand:
    """Classify *query* into an allowed read-path command.

    ``default_super`` supplies the schema for an unqualified ``SHOW STATS``
    target (matching ``SQLParser``'s default-super behaviour for SELECTs).

    Raises ``ValueError`` for a recognised-but-malformed command (e.g.
    ``SHOW STATS`` with no table, ``EXPLAIN`` of a non-SELECT).  An empty or
    non-EXPLAIN/non-SHOW input is returned as :data:`CommandKind.SELECT` with the
    raw text untouched so the existing pipeline handles (or rejects) it exactly
    as before.
    """
    raw = query or ""
    text = raw.strip()
    if not text:
        # Defer to the existing SQLParser, which raises the canonical
        # "non-empty SQL string" error — preserves current behaviour.
        return SystemCommand(kind=CommandKind.SELECT, sql=raw)

    m = _SHOW_STATS_RE.match(text)
    if m:
        body = m.group("body").strip().rstrip(";").strip()
        ref = _TABLE_REF_RE.match(body)
        if not ref:
            raise ValueError(
                "SHOW STATS expects a table reference: SHOW STATS [super.]simple"
            )
        super_name = (
            _unquote(ref.group("super")) if ref.group("super") else default_super
        )
        simple_name = _unquote(ref.group("simple"))
        return SystemCommand(
            kind=CommandKind.SHOW_STATS,
            super_name=super_name,
            simple_name=simple_name,
        )

    m = _EXPLAIN_RE.match(text)
    if m:
        inner = m.group("inner").strip()
        if not _SELECT_INNER_RE.match(inner):
            raise ValueError("EXPLAIN is only supported for SELECT statements.")
        options = "ANALYZE" if m.group("opts") else ""
        return SystemCommand(
            kind=CommandKind.EXPLAIN,
            sql=inner,
            explain=True,
            explain_options=options,
        )

    # Ordinary query — unchanged SELECT path (raw text preserved verbatim).
    return SystemCommand(kind=CommandKind.SELECT, sql=raw)
