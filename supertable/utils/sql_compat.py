# route: supertable.utils.sql_compat
"""Parser-compatibility shims for SQL the engine accepts but SQLGlot cannot parse.

WHY THIS EXISTS

Admission control (:func:`supertable.system_query.assert_read_only`) must parse
every query before it runs — the guard is an allow-list over the AST, so "we
could not parse it" has to mean "we refuse it". That makes the parser's coverage
the read path's coverage: anything SQLGlot cannot parse is unavailable, even
when DuckDB executes it happily.

Two such gaps were found in the 2026-09-14 read-path audit and are shimmed here.
Both were verified against DuckDB (the query is valid) *and* against a newer
SQLGlot, so each shim records whether it is a bug that will age out or a
permanent translation:

  * ``IS [NOT] UNKNOWN`` — rejected by SQLGlot 26.33. Fixed upstream in 27.x,
    which normalises it to exactly ``IS [NOT] NULL``; this module performs that
    same rewrite so the pinned 26.x range behaves identically. Retire this shim
    when the ``sqlglot<27`` pin is lifted.

  * A grouping extension (``ROLLUP`` / ``CUBE`` / ``GROUPING SETS``) immediately
    followed by ``LIMIT`` / ``OFFSET`` — rejected by 26.33 **and** 27.29, so
    this is an upstream bug with no known fix version. SQLGlot's GROUP BY parser
    consumes the ``LIMIT`` keyword as another grouping expression and then
    chokes on the count. Any intervening clause avoids it, which is what the
    shim exploits.

TEXT REWRITING IS TOKEN-DRIVEN, NOT REGEX-DRIVEN

These are rewrites of SQL *text*, because by definition the text does not parse
— there is no AST to manipulate. A regex over raw SQL would corrupt string
literals and comments (``SELECT 'IS UNKNOWN'`` is data, not syntax). SQLGlot's
tokenizer succeeds even when its parser fails and it classifies literals and
comments for us, so every rewrite below is driven by token types and spliced by
the tokenizer's own character offsets. Nothing is matched against raw text.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError, TokenError
from sqlglot.tokens import Token, TokenType


#: Grouping extensions whose argument list SQLGlot fails to terminate when the
#: next token is LIMIT/OFFSET.
_GROUPING_EXTENSIONS = (TokenType.ROLLUP, TokenType.CUBE, TokenType.GROUPING_SETS)

#: The clause keywords that trigger the grouping-extension parse failure.
_ROW_BOUND_TOKENS = (TokenType.LIMIT, TokenType.OFFSET)

#: Alias given to the subquery introduced by :func:`_wrap_row_bound`. Prefixed
#: so it cannot collide with a user alias.
_WRAP_ALIAS = "_st_grouping_limit"


def _tokenize(sql: str, dialect: str = "duckdb") -> Optional[List[Token]]:
    """Tokenize *sql*, or return ``None`` if even tokenization fails.

    Tokenization is far more permissive than parsing, so this normally succeeds
    on input the parser rejects — that asymmetry is the whole basis of this
    module. When it does fail there is nothing to shim and the caller must let
    the original parse error stand.
    """
    try:
        return sqlglot.tokenize(sql, read=dialect)
    except (TokenError, ParseError):
        return None


def _splice(sql: str, start: int, end: int, replacement: str) -> str:
    """Replace the inclusive ``[start, end]`` character span of *sql*."""
    return sql[:start] + replacement + sql[end + 1:]


# ---------------------------------------------------------------------------
# IS [NOT] UNKNOWN  ->  IS [NOT] NULL
# ---------------------------------------------------------------------------

def _rewrite_is_unknown(sql: str, dialect: str = "duckdb") -> str:
    """Rewrite ``IS [NOT] UNKNOWN`` predicates to ``IS [NOT] NULL``.

    The two are equivalent for every input: ``UNKNOWN`` *is* SQL's three-valued
    NULL for booleans, and SQLGlot 27.x normalises the former to the latter.

    Only an ``UNKNOWN`` token preceded by ``IS`` (optionally ``IS NOT``) is
    touched, so the ``UNKNOWN`` *type* name and any identifier or literal
    spelled the same are left alone.
    """
    tokens = _tokenize(sql, dialect)
    if not tokens:
        return sql

    spans = []
    for i, token in enumerate(tokens):
        if token.token_type is not TokenType.UNKNOWN:
            continue
        prev = tokens[i - 1] if i >= 1 else None
        if prev is not None and prev.token_type is TokenType.NOT:
            prev = tokens[i - 2] if i >= 2 else None
        if prev is not None and prev.token_type is TokenType.IS:
            spans.append((token.start, token.end))

    # Splice from the back so earlier offsets stay valid.
    for start, end in reversed(spans):
        sql = _splice(sql, start, end, "NULL")
    return sql


# ---------------------------------------------------------------------------
# LIMIT ALL  ->  no limit
# ---------------------------------------------------------------------------

def _strip_limit_all(sql: str, dialect: str = "duckdb") -> str:
    """Remove ``LIMIT ALL``, which is DuckDB/Postgres for *unbounded*.

    DuckDB executes ``LIMIT ALL`` happily, but SQLGlot has no node for it and
    parses the bare word as ``Limit(expression=Column(ALL))``. That costs twice:

      1. The column sweep in ``SQLParser`` collected ``ALL`` as a required data
         column and the estimator refused the query with
         ``Missing required column(s): warehouse.orders: ALL``.
      2. Rendering the AST back out emits ``LIMIT "ALL"`` — a *quoted
         identifier* — so once the table-hashing rewrite round-tripped the
         query, DuckDB failed to bind a column named ALL. Fixing only (1) left
         this one, because the clause is corrupted by the reparse, not by the
         column analysis.

    Deleting the clause fixes both at the source: a removed row bound is
    exactly what ``LIMIT ALL`` requests, and ``query_sql`` then applies its
    default cap to it like any other unbounded query.

    Matched on tokens — ``TokenType.ALL`` immediately after ``TokenType.LIMIT``
    at paren depth 0 — so ``GROUP BY ALL`` and ``ORDER BY ALL`` (which SQLGlot
    models properly) are untouched, as is a subquery's own ``LIMIT ALL``.
    """
    tokens = _tokenize(sql, dialect)
    if not tokens:
        return sql

    depth = 0
    spans = []
    for i, token in enumerate(tokens):
        if token.token_type is TokenType.L_PAREN:
            depth += 1
        elif token.token_type is TokenType.R_PAREN:
            depth -= 1
        elif (
            depth == 0
            and token.token_type is TokenType.LIMIT
            and i + 1 < len(tokens)
            and tokens[i + 1].token_type is TokenType.ALL
        ):
            spans.append((token.start, tokens[i + 1].end))

    for start, end in reversed(spans):
        sql = _splice(sql, start, end, "").rstrip()
    return sql


# ---------------------------------------------------------------------------
# GROUP BY ROLLUP/CUBE/GROUPING SETS (...) LIMIT n [OFFSET m]
# ---------------------------------------------------------------------------

def _wrap_row_bound(sql: str, dialect: str = "duckdb") -> Optional[str]:
    """Move a trailing ``LIMIT``/``OFFSET`` outside a grouping-extension query.

    ``SELECT ... GROUP BY ROLLUP (a, b) LIMIT 100`` becomes
    ``SELECT * FROM (SELECT ... GROUP BY ROLLUP (a, b)) AS _st_grouping_limit
    LIMIT 100``, which SQLGlot parses, re-parses, and renders for either
    dialect.

    Returns ``None`` when the query does not have this shape, so the caller
    keeps the original parse error rather than reporting a rewrite artefact.

    ORDERING IS NOT AT RISK. A row bound without ORDER BY selects an
    implementation-defined subset, so in general moving it across a subquery
    boundary could change *which* rows come back. It cannot here: the parse
    failure only occurs when the row bound *immediately* follows the grouping
    extension, and any intervening clause — ORDER BY included — parses fine.
    So every query reaching this function provably has no ORDER BY to preserve.
    """
    tokens = _tokenize(sql, dialect)
    if not tokens:
        return None

    depth = 0
    seen_group_by = False
    grouping_at_depth: Optional[int] = None
    split_index: Optional[int] = None

    for i, token in enumerate(tokens):
        ttype = token.token_type
        if ttype is TokenType.L_PAREN:
            depth += 1
            continue
        if ttype is TokenType.R_PAREN:
            depth -= 1
            continue
        if ttype is TokenType.GROUP_BY:
            seen_group_by = True
            continue
        if seen_group_by and ttype in _GROUPING_EXTENSIONS:
            grouping_at_depth = depth
            continue
        # The row bound must be at the same nesting level as the grouping
        # extension; a LIMIT inside a subquery is somebody else's problem.
        if grouping_at_depth is not None and depth == grouping_at_depth \
                and ttype in _ROW_BOUND_TOKENS:
            split_index = i
            break

    if split_index is None:
        return None

    head = sql[:tokens[split_index].start].rstrip()
    tail = sql[tokens[split_index].start:].strip()
    # A terminal semicolon belongs to the statement, not to the row bound.
    tail = tail.rstrip().rstrip(";").rstrip()
    if not head or not tail:
        return None

    return f"SELECT * FROM ({head}) AS {_WRAP_ALIAS} {tail}"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _normalize_text(sql: str, dialect: str) -> str:
    """Apply the token-level rewrites, tokenizing only when one could apply.

    Both rewrites need the tokenizer to be *correct* — that is what keeps them
    off string literals and comments. But neither can apply unless the keyword
    is present in the text at all, and a case-insensitive substring test is
    essentially free where tokenizing is not: on a query carrying a 1,000-value
    IN list, each tokenization costs ~25ms, and admission runs on every read.

    The gate is a necessary condition, never a sufficient one. A query
    containing the word in a literal still gets tokenized and still comes back
    unchanged — the cost of a false positive is a wasted tokenization, not a
    wrong rewrite. The tokenizer remains the only thing that decides.
    """
    upper = sql.upper()
    if "UNKNOWN" in upper:
        sql = _rewrite_is_unknown(sql, dialect)
        upper = sql.upper()
    # ``LIMIT ALL`` needs both words; requiring LIMIT alone would tokenize
    # every bounded query for nothing.
    if "ALL" in upper and "LIMIT" in upper:
        sql = _strip_limit_all(sql, dialect)
    return sql

def normalize_and_parse(
    sql: str, dialect: str = "duckdb",
) -> Tuple[str, List[exp.Expression]]:
    """Normalize *sql* and parse it, in ONE parse. ``(text, statements)``.

    Normalizing requires a parse — that is how "does this need the
    grouping-extension wrap?" is answered — so handing the caller only the text
    and letting it parse again pays for the same work twice. On a query with a
    1,000-value IN list that second parse cost ~190ms, and the read benchmark's
    ``random_1000_by_key`` scenario regressed ~200ms because of it. The same
    double parse had been removed from this path once before, deliberately;
    this returns both halves so it cannot come back.

    Unconditional rewrites (``IS UNKNOWN``, ``LIMIT ALL``) are applied first
    because they are pure translations. The grouping-extension wrap is applied
    **only** after a real parse failure, so a query that already parses is
    returned byte-for-byte unchanged and cannot be perturbed by this shim.

    Raises :class:`sqlglot.errors.ParseError` when the text does not parse even
    after normalization, so the caller decides how that is reported.
    """
    if not sql or not sql.strip():
        return sql, []

    normalized = _normalize_text(sql, dialect)

    try:
        return normalized, _executable(sqlglot.parse(normalized, read=dialect))
    except ParseError:
        wrapped = _wrap_row_bound(normalized, dialect)
        if wrapped is None:
            raise
        try:
            return wrapped, _executable(sqlglot.parse(wrapped, read=dialect))
        except ParseError:
            # Report the failure of the text the caller actually gave us, not
            # of a rewrite it never asked for.
            raise


def _executable(statements: List[Optional[exp.Expression]]) -> List[exp.Expression]:
    return [st for st in statements if not is_noop_statement(st)]


def normalize_read_sql(sql: str, dialect: str = "duckdb") -> str:
    """The text half of :func:`normalize_and_parse`, for callers with no use
    for the AST.

    Unparseable input comes back unchanged rather than raising: these callers
    are not the ones that decide admissibility, and admission will report the
    error properly.
    """
    try:
        return normalize_and_parse(sql, dialect)[0]
    except ParseError:
        return sql


def is_noop_statement(statement: Optional[exp.Expression]) -> bool:
    """True when *statement* carries no executable work.

    SQLGlot represents a statement terminator that has comments attached to it —
    ``SELECT 1; -- done`` — as a trailing :class:`exp.Semicolon` node. Counting
    that as a statement made a single commented SELECT look like a two-statement
    chain and got it refused. A comment is not a statement.

    Genuine chains are unaffected: ``SELECT 1; SELECT 2`` parses to two
    ``Select`` nodes and ``SELECT 1; DROP TABLE t`` to a ``Select`` and a
    ``Drop``, none of which are no-ops.
    """
    if statement is None:
        return True
    if isinstance(statement, exp.Semicolon):
        return True
    return False


def parse_read_statements(sql: str, dialect: str = "duckdb") -> List[exp.Expression]:
    """Parse *sql* into its executable statements, dropping no-op nodes.

    Raises :class:`sqlglot.errors.ParseError` so the caller decides how an
    unparseable query is reported.
    """
    return [
        st for st in sqlglot.parse(sql, read=dialect)
        if not is_noop_statement(st)
    ]


def parse_read_one(sql: str, dialect: str = "duckdb") -> Optional[exp.Expression]:
    """Parse a single-statement read query, or return ``None`` if it will not parse.

    Applies :func:`normalize_read_sql` first. Intended for callers that want to
    *inspect* a query (e.g. to find its row bound) and can fall back to leaving
    the text alone — it deliberately never raises, because such callers are not
    the ones that decide whether the query is admissible.
    """
    if not sql or not sql.strip():
        return None
    try:
        _text, statements = normalize_and_parse(sql, dialect)
    except Exception:
        return None
    if len(statements) != 1:
        return None
    return statements[0]
