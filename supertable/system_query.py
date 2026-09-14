# route: supertable.system_query
"""Read-path command classification.

The read path is intentionally restricted: ``DataReader`` only resolves and runs
queries that read existing data.  Historically this was enforced implicitly —
anything that wasn't a ``SELECT`` failed somewhere downstream (``EXPLAIN`` /
``SHOW`` parse to a sqlglot ``Command`` with no tables, so ``SQLParser`` raised
its table-free-query error).

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

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Tuple

from supertable.utils.sql_compat import normalize_read_sql, parse_read_statements


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
    #: The sqlglot AST of ``sql``, parsed with the duckdb dialect during
    #: admission. Handed on so ``SQLParser`` does not parse the same text a
    #: second time — on a query with a 1,000-value IN list that second parse
    #: cost 103ms. Ownership transfers with it: admission keeps no reference,
    #: so there is one owner and no shared mutable AST. ``None`` when the
    #: statement was not parsed (SHOW STATS, or an empty query).
    parsed: Any = None
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


# ---------------------------------------------------------------------------
# Read-path admission control
# ---------------------------------------------------------------------------

#: Statement roots a read may have. Anything else — COPY, ATTACH, INSTALL, SET,
#: PRAGMA, CALL, EXPORT, DDL, DML — is not a read and is refused outright.
_READ_ROOTS = (exp.Select, exp.Union, exp.Intersect, exp.Except, exp.Subquery)


def assert_read_only(sql: str):
    """Refuse anything that is not a plain read of real tables.

    WHY THIS EXISTS

    Everything that protects a row or a column in this library lives in a VIEW
    the reader builds: the deletion vector is an anti-join, RBAC is a filtered
    view over the reflection, and a share filter is merged into that view. A
    query that never references those views is not restricted by them — it is
    simply outside the system.

    DuckDB can read files directly, so before this guard existed a query could
    step around the whole chain:

        SELECT p.* FROM read_parquet(['<data file>']) p
         WHERE EXISTS (SELECT 1 FROM orders)

    The EXISTS clause names one real table, which was enough for the query to be
    accepted; the projection then came from the raw parquet. That returned a
    masked column, __rowid__, and a row the deletion vector had removed. On
    object storage it reads with the engine's own credentials, so it crosses
    tenants. ``read_csv('/etc/hostname')`` and ``COPY ... TO`` were reachable the
    same way.

    ALLOW-LIST, NOT BLOCK-LIST

    Naming the dangerous functions would be a losing game — DuckDB adds more,
    and an extension adds its own. The rule is instead positive and small: the
    statement must be a read, and every FROM/JOIN source must be a named table.

    The AST makes that distinction cleanly. A real table parses as
    ``Table(this=Identifier)``; every table function parses as ``Table`` whose
    ``this`` is a function node instead. So "is this an identifier" is the whole
    test, and it holds for forms nobody has thought of yet.

    Raises ValueError, which the reader already turns into Status.ERROR.

    Returns the parsed statement so the caller can reuse it rather than parse
    the same text again.
    """
    return _admit_read_sql(sql)[0]


def _admit_read_sql(sql: str) -> Tuple[Any, str]:
    """Admission-check *sql* and return ``(root_ast, admitted_sql)``.

    ``admitted_sql`` is the text the AST was parsed from, which is *not*
    necessarily the input: :func:`normalize_read_sql` first rewrites constructs
    the installed SQLGlot cannot parse but the engine accepts (see
    ``supertable.utils.sql_compat``). The caller must pass that text downstream,
    not the original, so the AST and the executed SQL stay the same query.

    The whole contract of :func:`assert_read_only` applies here — this exists
    only so ``classify_query`` can get the normalized text without normalizing
    and parsing a second time.
    """
    raw = sql or ""
    text = raw.strip()
    if not text:
        return None, sql            # empty defers to SQLParser's own error

    normalized = normalize_read_sql(text)
    # Hand back the caller's own text byte-for-byte unless a shim actually
    # fired. Normalization is a last resort for SQL that would otherwise be
    # refused; it must not quietly reformat every query that passes through.
    admitted = raw if normalized == text else normalized
    text = normalized

    try:
        # parse_read_statements drops statement nodes that carry no executable
        # work. A terminal semicolon with a trailing comment parses as its own
        # exp.Semicolon node, and counting that made "SELECT ...; -- note" look
        # like a two-statement chain and refused it. A comment is not a
        # statement; a real second statement still parses as one.
        statements = parse_read_statements(text)
    except ParseError as e:
        # Unparseable SQL is refused here rather than handed to the engine:
        # "the parser could not read it" must not mean "let DuckDB try".
        raise ValueError(f"could not parse query: {e}") from e

    if not statements:
        return None, admitted
    if len(statements) > 1:
        # One request is one statement. Anything else is a chain, and a chain is
        # how an injected payload arrives.
        raise ValueError(
            "only a single statement may be submitted; found "
            f"{len(statements)}"
        )

    root = statements[0]
    if not isinstance(root, _READ_ROOTS):
        raise ValueError(
            f"{type(root).__name__.upper()} is not permitted on the read path; "
            f"only SELECT queries are"
        )

    for table in root.find_all(exp.Table):
        inner = table.this
        if not isinstance(inner, exp.Identifier):
            # A table function: read_parquet, read_csv, glob, an extension's
            # own, or one that does not exist yet.
            name = type(inner).__name__ if inner is not None else "?"
            raise ValueError(
                "only named tables may be queried; table functions such as "
                f"{name.lower()}() are not permitted on the read path"
            )

    return root, admitted


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
        # EXPLAIN reaches the same engine with the same text, so it is admitted
        # on the same terms — otherwise it is a hole the shape of the guard.
        inner_ast, inner_sql = _admit_read_sql(inner)
        options = "ANALYZE" if m.group("opts") else ""
        return SystemCommand(
            kind=CommandKind.EXPLAIN,
            sql=inner_sql,
            parsed=inner_ast,
            explain=True,
            explain_options=options,
        )

    # Ordinary query. Admission-checked first: until this guard existed, any
    # text that named one real table ran verbatim, which put DuckDB's file
    # functions inside the read path and outside every access control.
    #
    # ``sql`` is the *admitted* text, not ``raw``: admission may have rewritten
    # a construct SQLGlot cannot parse into an equivalent it can, and the
    # executor has to run the query the AST actually describes.
    root, admitted_sql = _admit_read_sql(raw)
    return SystemCommand(kind=CommandKind.SELECT, sql=admitted_sql, parsed=root)
