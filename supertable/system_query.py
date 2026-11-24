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

import functools
import json
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Tuple

from supertable.utils.sql_compat import normalize_and_parse


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

#: Nodes that are not a read wherever they stand, not only at the root. A read
#: root can carry one: ``SELECT * INTO x FROM t`` is a Select whose ``into``
#: SQLGlot writes back out as ``CREATE TABLE x AS SELECT ...``, and a CTE body
#: parses as any statement at all (``WITH w AS (DELETE FROM t RETURNING *)``).
#: Built by name so a class the pinned SQLGlot lacks is skipped, not an
#: import error.
_NOT_A_READ = tuple(
    cls for cls in (
        getattr(exp, name, None) for name in (
            "DML", "DDL", "Into", "Lock", "Command", "Transaction", "Pragma",
            "Set", "Attach", "Detach", "Copy", "Use", "Describe",
        )
    ) if cls is not None
)

#: What may stand after LATERAL besides a subquery, VALUES or UNNEST: functions
#: that make rows out of the values they are handed and open nothing. DuckDB's
#: ``range`` / ``generate_series`` and its JSON walkers, and the generators of
#: Spark's ``LATERAL VIEW``. This is the allow-list; every other function there
#: is refused, known or not.
_LATERAL_ROW_GENERATORS = frozenset({
    "unnest", "range", "generate_series",
    "explode", "explode_outer", "posexplode", "posexplode_outer",
    "inline", "inline_outer", "stack", "json_tuple",
    "json_each", "json_tree",
})

#: Scalar functions that read the engine's own state instead of table data:
#: settings (where an object store's keys can sit), variables, secrets, the
#: catalog of views the reader built — a view's SQL names the files under it —
#: the engine's log, or the path of the file a row came from.
_ENGINE_STATE_FUNCTIONS = frozenset({
    "current_setting", "getenv", "getvariable", "which_secret",
    "pg_get_viewdef", "pg_get_constraintdef", "get_block_size", "write_log",
    "input_file_name", "input_file_block_start", "input_file_block_length",
})

#: Spark's (and Hive's) ways to call into the JVM from SQL.
_JAVA_CALL_FUNCTIONS = frozenset({
    "reflect", "reflect2", "java_method", "try_reflect",
})

#: Functions that open a file or a URL, or run SQL of their own. Most are
#: table functions, which the FROM and LATERAL rules already refuse by shape;
#: they are named here as well so that one is refused wherever a grammar lets
#: it stand — a projection, an argument, behind a schema qualifier.
_FILE_READING_FUNCTIONS = frozenset({
    "glob", "query", "query_table", "sniff_csv", "in_file",
    "parquet_scan", "csv_scan", "json_scan", "ndjson_scan", "arrow_scan",
    "sqlite_scan", "sqlite_attach", "postgres_scan", "postgres_attach",
    "mysql_scan", "iceberg_scan", "iceberg_metadata", "iceberg_snapshots",
    "delta_scan", "load_extension", "load_aws_credentials",
    "json_execute_serialized_sql", "shapefile_meta",
})

#: ...and every function whose name starts with one of these: ``read_text``,
#: ``read_parquet``, ``parquet_metadata``, ``duckdb_secrets``,
#: ``pragma_database_size``, ``http_get``. An engine or an extension adds
#: readers faster than a list of names is kept; these families are the ones
#: that have never held anything a read of table data needs.
_FILE_READING_PREFIXES = (
    "read_", "parquet_", "duckdb_", "pragma_", "sqlite_", "postgres_",
    "mysql_", "iceberg_", "delta_", "http", "s3_", "azure_", "gcs_", "st_read",
)


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
    statement must be a read all the way down, every FROM/JOIN source must be a
    named table, and a LATERAL may read only a subquery, VALUES, UNNEST or a
    function that makes rows of the values it is given.

    The AST makes that distinction cleanly. A real table parses as
    ``Table(this=Identifier)``; every table function parses as ``Table`` whose
    ``this`` is a function node instead. So "is this an identifier" is the whole
    test, and it holds for forms nobody has thought of yet.

    A function after LATERAL is not a ``Table`` at all — it parses as
    ``Lateral(this=<function>)`` — so until 3.5.4 that test never saw it, and
    ``FROM t, LATERAL read_text('/etc/hostname')`` ran for any role that could
    read ``t``. LATERAL is judged on its own now, against
    :data:`_LATERAL_ROW_GENERATORS`: a list of what is allowed, so a table
    function nobody has heard of is refused with the rest. A name behind a
    schema qualifier (``main.range(3)``) or in quotes is refused there too;
    neither is how anyone writes a row generator.

    THE ONE BLOCK-LIST, AND WHY IT IS THERE

    Scalar functions cannot be allow-listed: there are hundreds and a read
    legitimately uses any of them. The few that read something other than the
    values they are handed are refused by name wherever they stand
    (:data:`_ENGINE_STATE_FUNCTIONS`, :data:`_JAVA_CALL_FUNCTIONS`,
    :data:`_FILE_READING_FUNCTIONS`, :data:`_FILE_READING_PREFIXES`), and so is
    Spark's ``TRANSFORM ... USING``, which runs a program.

    WHAT IS JUDGED IS WHAT RUNS

    The engine never receives the caller's text. It receives what SQLGlot
    writes back out from this parse, with table names replaced. Two things
    follow, and both were holes:

      * A comment is not in the parse, so it is not judged — and SQLGlot wrote
        a line comment back out as ``/* text */`` without escaping a ``*/``
        inside the text. ``SELECT a -- */ , (SELECT content FROM
        read_text('/x')) /*`` was one column and a comment here, and two
        columns at the engine. Comments are cleared from the statement this
        returns, and every writer downstream writes none.
      * There is one reading. Spark's grammar ends a string elsewhere than
        DuckDB's (``\\'``), so a second parse of the same text for the Spark
        engine could name different tables than the statement Spark was then
        sent. Nothing parses the caller's text as Spark any more.

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

    try:
        # ONE parse for both halves. Normalizing needs a parse to decide whether
        # a rewrite is required, so asking for the text and then parsing it
        # again paid for the same work twice — ~190ms on a 1,000-value IN list.
        #
        # The statements exclude nodes carrying no executable work: a terminal
        # semicolon with a trailing comment parses as its own exp.Semicolon
        # node, and counting that made "SELECT ...; -- note" look like a
        # two-statement chain and refused it. A comment is not a statement; a
        # real second statement still parses as one.
        normalized, statements = normalize_and_parse(text)
    except ParseError as e:
        # Unparseable SQL is refused here rather than handed to the engine:
        # "the parser could not read it" must not mean "let DuckDB try".
        raise ValueError(f"could not parse query: {e}") from e

    # Hand back the caller's own text byte-for-byte unless a shim actually
    # fired. Normalization is a last resort for SQL that would otherwise be
    # refused; it must not quietly reformat every query that passes through.
    admitted = raw if normalized == text else normalized

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

    _judge(root)
    return root, admitted


def _function_name(node: exp.Expression) -> str:
    """A call's name, casefolded, however it is spelt.

    A function SQLGlot has no class for is an ``Anonymous`` whose ``this`` is
    the name as written — a plain string, or an ``Identifier`` when it was
    quoted (``"current_setting"('threads')``). The engine resolves both to the
    same function, so both read as the same name here.
    """
    if isinstance(node, exp.Anonymous):
        target = node.this
        if isinstance(target, exp.Identifier):
            target = target.this
        return str(target or "").strip().casefold()
    try:
        return str(node.sql_name() or "").casefold()
    except Exception:  # a node with no SQL name of its own
        return type(node).__name__.casefold()


def _refuse_function(name: str) -> None:
    """Refuse the function *name* when it reads anything but the values it is
    handed. See the lists for what each family is."""
    if name in _ENGINE_STATE_FUNCTIONS:
        raise ValueError(
            f"{name}() reads the engine's own state and is not permitted on "
            f"the read path"
        )
    if name in _JAVA_CALL_FUNCTIONS:
        raise ValueError(
            f"{name}() calls Java and is not permitted on the read path"
        )
    if name in _FILE_READING_FUNCTIONS or name.startswith(_FILE_READING_PREFIXES):
        raise ValueError(
            f"{name}() reads files, URLs or the engine's catalog and is not "
            f"permitted on the read path; only named tables may be queried"
        )


def _judge_lateral(node: exp.Lateral) -> None:
    """Refuse a LATERAL whose source is not on the allow-list.

    A subquery (or VALUES) is admitted here because its own tables and
    functions are walked like any other's. A function is admitted only when it
    is a row generator written plainly: not behind a qualifier — ``Dot`` is
    how ``main.read_text(...)`` parses — and not a quoted name. Anything else
    in this position (a bare name, a literal) reads something this module
    cannot name, so it is refused as well.
    """
    source = node.this
    if isinstance(source, (exp.Subquery, exp.Query, exp.Values, exp.Unnest)):
        return
    if isinstance(source, exp.Func):
        quoted = isinstance(source, exp.Anonymous) and isinstance(source.this, exp.Identifier)
        name = _function_name(source)
        if not quoted and name in _LATERAL_ROW_GENERATORS:
            return
        raise ValueError(
            "LATERAL may read a subquery, VALUES, UNNEST or a row generator "
            f"such as range(); {name or '?'}() is not permitted on the read path"
        )
    raise ValueError(
        "LATERAL may read a subquery, VALUES, UNNEST or a row generator such "
        "as range(); anything else is not permitted on the read path"
    )


def _judge(root: exp.Expression) -> None:
    """Hold every node of an admitted statement to the read path's rules.

    One walk, because admission runs on every read and a query with a
    1,000-value IN list has a thousand nodes to visit. Comments are cleared on
    the way: see "WHAT IS JUDGED IS WHAT RUNS" on :func:`assert_read_only`.
    """
    for node in root.walk():
        if node.comments:
            node.comments = None

        if isinstance(node, exp.Table):
            inner = node.this
            if not isinstance(inner, exp.Identifier):
                # A table function: read_parquet, read_csv, glob, an
                # extension's own, or one that does not exist yet.
                name = type(inner).__name__ if inner is not None else "?"
                raise ValueError(
                    "only named tables may be queried; table functions such as "
                    f"{name.lower()}() are not permitted on the read path"
                )
        elif isinstance(node, exp.Lateral):
            _judge_lateral(node)
        elif isinstance(node, exp.Func):
            _refuse_function(_function_name(node))
        elif isinstance(node, exp.QueryTransform):
            raise ValueError(
                "TRANSFORM ... USING runs a program on the engine's machine "
                "and is not permitted on the read path"
            )
        elif isinstance(node, _NOT_A_READ):
            raise ValueError(
                f"{type(node).__name__.upper()} is not permitted on the read "
                f"path; only SELECT queries are"
            )


# ---------------------------------------------------------------------------
# Row predicates that are not the caller's SQL
# ---------------------------------------------------------------------------

#: Stands in for the table a row predicate is judged against, and names the
#: call its text is handed to DuckDB's parser in.  Neither exists anywhere.
_ROW_PREDICATE_NAME = "__st_row_predicate__"
_ROW_PREDICATE_SELECT = f"SELECT 1 FROM {_ROW_PREDICATE_NAME} WHERE "

_NOT_ONE_EXPRESSION = "the row predicate is not a single SQL expression"
_READS_ELSEWHERE = (
    "a row predicate may read the row it filters and nothing else; "
    "subqueries are not permitted in one"
)


def admit_row_predicate(predicate: Any) -> str:
    """Admit a row predicate that reaches a view as text, and return it.

    WHY THIS EXISTS

    A linked share carries a row filter (``_row_filter``) on its snapshot, and
    the reader writes that text into the ``WHERE`` of the view every row of the
    share is read through. It is SQL that nobody submitted to the read gate:
    :func:`assert_read_only` judges the caller's statement, and the rewrite
    that refuses an unknown table only ever sees the caller's statement too.
    Written into the view as it came, the text was held to no rule at all:

      * ``1=1; CREATE TABLE x AS SELECT ...`` ran its second statement on the
        reader's connection;
      * ``1=1) OR (1=1`` closed the parenthesis it was merged in and undid the
        role's own row filter beside it;
      * ``id IN (SELECT ... FROM <another view on the connection>)`` read a
        relation no role of the caller was checked for.

    WHAT IS ADMITTED

    One expression over the row being filtered: columns, literals, operators
    and scalar functions. Nothing that reads another relation, so no subquery
    in any form, and no function the read gate refuses a caller
    (:func:`_refuse_function`). A predicate that needs anything else was never
    going to work: the tables it could name are not on the reader's connection.

    WHAT IS JUDGED IS WHAT RUNS, FOR BOTH ENGINES

    The text is returned UNCHANGED, so a predicate that is admitted means
    exactly what it meant before this check existed. Writing SQLGlot's reading
    back out instead would have been the obvious way to make "judged" and
    "run" the same thing, and it is what the caller's statement gets, but it
    changes what some predicates mean: DuckDB's ``name ~ 'x'`` matches the
    whole string and comes back out as ``REGEXP_MATCHES``, which matches any
    part of it. A row filter that quietly lets more rows through is the fault
    this check is here to prevent.

    So each engine's own reading is judged instead:

      * DuckDB runs the text as it is, and DuckDB's parser is asked how it
        reads it (:func:`_judge_row_predicate_as_duckdb_reads_it`).
      * Spark runs SQLGlot's writing of it (``_to_spark_predicate``), and
        SQLGlot's reading is judged
        (:func:`_judge_row_predicate_as_sqlglot_reads_it`).

    Both have to admit it. A predicate only one of them can read is refused;
    which engine a read runs on is not known here.

    Raises ``ValueError``. The message never quotes the predicate: it is the
    provider's policy, and whoever is refused the read is not its author.
    Admitted text is remembered, so a share's predicate is parsed once per
    process and not once per read.
    """
    if not isinstance(predicate, str) or not predicate.strip():
        raise ValueError("a row predicate has to be a non-empty SQL expression")
    return _admit_row_predicate(predicate)


@functools.lru_cache(maxsize=1024)
def _admit_row_predicate(text: str) -> str:
    # Only a predicate that was admitted is remembered: lru_cache keeps no
    # entry for a call that raised, so a refused one is judged again each time.
    _judge_row_predicate_as_sqlglot_reads_it(text)
    _judge_row_predicate_as_duckdb_reads_it(text)
    return text


def _judge_row_predicate_as_sqlglot_reads_it(text: str) -> None:
    """Hold SQLGlot's reading of *text* to the rules of a row predicate.

    The text is read as the ``WHERE`` of a statement that has nothing else.
    If what SQLGlot writes back is that statement and its condition, the text
    was a condition and only that: a second statement, a set operation or a
    clause after it (``GROUP BY``, ``ORDER BY``, ``LIMIT``, ``QUALIFY``) each
    leave something more to write.
    """
    try:
        _normalized, statements = normalize_and_parse(_ROW_PREDICATE_SELECT + text)
    except Exception:
        # Not chained: the parser's message quotes the text.
        raise ValueError(
            "the row predicate is not a SQL expression the read path can parse"
        ) from None

    if len(statements) != 1 or type(statements[0]) is not exp.Select:
        raise ValueError(_NOT_ONE_EXPRESSION)
    root = statements[0]

    # The gate's own walk: table functions, LATERAL, the functions that read
    # the engine's state, files or the JVM. It clears comments on the way.
    _judge(root)

    where = root.args.get("where")
    condition = where.this if where is not None else None
    if condition is None:
        raise ValueError(_NOT_ONE_EXPRESSION)
    written = root.sql(dialect="duckdb", comments=False)
    if written != _ROW_PREDICATE_SELECT + condition.sql(dialect="duckdb", comments=False):
        raise ValueError(_NOT_ONE_EXPRESSION)

    for node in condition.walk():
        if isinstance(node, (exp.Query, exp.Subquery, exp.Table, exp.Lateral)):
            raise ValueError(_READS_ELSEWHERE)


def _judge_row_predicate_as_duckdb_reads_it(text: str) -> None:
    """Hold DuckDB's own reading of *text* to the rules of a row predicate.

    DuckDB is the engine that runs the text as it is, so it is DuckDB's parser
    that has to say the text is one expression. It is handed the text as the
    single argument of a call, ``__st_row_predicate__(<text>)``, and its parse
    tree is compared with the tree of the same call around ``TRUE``: the two
    may differ in that one argument and nowhere else. Text that closes the
    call, adds an argument, a clause or a statement leaves a different tree,
    or none.

    Nothing is run. ``json_serialize_sql`` parses; the statement is its
    parameter, on a connection of its own that holds no table and no secret.
    """
    try:
        import duckdb

        con = duckdb.connect()
        try:
            try:
                # A missing extension is an error here, never a download.
                con.execute("SET autoinstall_known_extensions=false;")
            except Exception:
                pass
            baseline = _duckdb_reading(
                con, f"SELECT 1 WHERE {_ROW_PREDICATE_NAME}(TRUE)")
            candidate = _duckdb_reading(
                con, f"SELECT 1 WHERE {_ROW_PREDICATE_NAME}({text})")
        finally:
            con.close()
    except ValueError:
        raise
    except Exception as e:
        # Not chained, for the reason given above; the class says enough.
        raise ValueError(
            "DuckDB could not be asked how it reads the row predicate "
            f"({type(e).__name__})"
        ) from None

    call = _row_predicate_call(candidate)
    arguments = call.get("children")
    if not isinstance(arguments, list) or len(arguments) != 1:
        raise ValueError(_NOT_ONE_EXPRESSION)
    argument = arguments[0]

    # Everything but the argument has to be what the bare call parses to.
    call["children"] = _row_predicate_call(baseline)["children"]
    if candidate != baseline:
        raise ValueError(_NOT_ONE_EXPRESSION)

    pending = [argument]
    while pending:
        item = pending.pop()
        if isinstance(item, dict):
            if item.get("class") == "SUBQUERY" or "subquery" in item:
                if not _selects_from_nothing(item.get("subquery")):
                    raise ValueError(_READS_ELSEWHERE)
            name = item.get("function_name")
            if isinstance(name, str) and name:
                _refuse_function(name.casefold())
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)


def _selects_from_nothing(subquery: Any) -> bool:
    """True when DuckDB's subquery node is a SELECT over no relation.

    DuckDB reads ``x = ANY([1, 2])`` and ``x IN (VALUES (1), (2))`` as
    subqueries: a SELECT with no FROM over the unnested list, and one over a
    list of literal rows. Neither names a table, a view or a function that
    opens one, so both stay admitted; what such a SELECT calls and holds is
    still walked by the caller. A subquery with any other source reads a
    relation, and so does one this does not recognise.
    """
    node = subquery.get("node") if isinstance(subquery, dict) else None
    if not isinstance(node, dict) or node.get("type") != "SELECT_NODE":
        return False
    source = node.get("from_table")
    if not isinstance(source, dict) or source.get("type") not in ("EMPTY", "EXPRESSION_LIST"):
        return False
    ctes = node.get("cte_map")
    if ctes and not (isinstance(ctes, dict) and not ctes.get("map")):
        return False
    return True


def _duckdb_reading(con: Any, sql: str) -> Any:
    """DuckDB's parse tree of the one statement *sql* holds, without the
    positions in the text, which differ for every predicate."""
    row = con.execute("SELECT json_serialize_sql(?::VARCHAR)", [sql]).fetchone()
    document = json.loads(row[0])
    statements = document.get("statements") if isinstance(document, dict) else None
    if (not isinstance(document, dict) or document.get("error")
            or not isinstance(statements, list) or len(statements) != 1):
        raise ValueError(_NOT_ONE_EXPRESSION)
    return _without_locations(statements[0])


def _without_locations(node: Any) -> Any:
    if isinstance(node, dict):
        return {key: _without_locations(value)
                for key, value in node.items() if key != "query_location"}
    if isinstance(node, list):
        return [_without_locations(value) for value in node]
    return node


def _row_predicate_call(statement: Any) -> dict:
    """The ``__st_row_predicate__(...)`` call that is the whole ``WHERE``."""
    node = statement.get("node") if isinstance(statement, dict) else None
    where = node.get("where_clause") if isinstance(node, dict) else None
    if (not isinstance(where, dict) or where.get("class") != "FUNCTION"
            or str(where.get("function_name") or "").casefold() != _ROW_PREDICATE_NAME):
        raise ValueError(_NOT_ONE_EXPRESSION)
    return where


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
