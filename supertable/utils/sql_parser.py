# route: supertable.utils.sql_parser
from datetime import date, datetime, timezone
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError
from sqlglot.optimizer.scope import traverse_scope
from supertable.data_classes import JoinEdge, PredInterval, TableDefinition


@dataclass(frozen=True)
class _ScopedTableBindings:
    """Stable internal keys for physical/derived sources in SQL scopes.

    SQL aliases are local to a SELECT scope.  The same spelling can therefore
    legitimately identify different relations in independent set-operation or
    subquery scopes.  Catalog reflection maps, however, need one request-wide
    key per distinct binding.  ``by_node_id`` bridges those two models without
    changing the alias that remains visible in the user's SQL.
    """

    by_node_id: Dict[int, str]
    alias_to_table: Dict[str, Tuple[str, str]]
    cte_reference_node_ids: Set[int]
    physical_keys: Set[str]


def _build_scoped_table_bindings(
    parsed: exp.Expression,
    default_super_name: str,
    *,
    scopes: Optional[Tuple[Any, ...]] = None,
) -> _ScopedTableBindings:
    """Assign request-wide keys while respecting SQL scope boundaries.

    Identical aliases bound to the same relation continue to share a key (and
    therefore a reflection).  An alias rebound to another relation in an
    independent scope receives an opaque collision-proof key.  Duplicate
    bindings inside one scope remain invalid and fail closed.
    """
    if scopes is None:
        try:
            scopes = tuple(traverse_scope(parsed))
        except Exception as exc:
            raise ValueError("Unable to resolve SQL table scopes") from exc

    table_scope: Dict[int, int] = {}
    cte_references: Set[int] = set()
    for scope_index, scope in enumerate(scopes):
        try:
            selected_sources = scope.selected_sources.values()
        except Exception as exc:
            # sqlglot reports duplicate aliases in a SELECT while constructing
            # selected_sources.  Such a query cannot have an unambiguous
            # protected-source rewrite.
            raise ValueError("Table alias is ambiguous within a SQL scope") from exc
        for selected in selected_sources:
            try:
                node, source = selected
            except (TypeError, ValueError):
                continue
            if not isinstance(node, exp.Table):
                continue
            table_scope[id(node)] = scope_index
            if not isinstance(source, exp.Table):
                cte_references.add(id(node))

    tables = list(parsed.find_all(exp.Table))
    reserved_folds = {
        (SQLParser._get_alias(table) or table.name).casefold()
        for table in tables
        if table.name
    }
    by_node_id: Dict[int, str] = {}
    alias_to_table: Dict[str, Tuple[str, str]] = {}
    used_key_folds: Set[str] = set()
    physical_keys: Set[str] = set()
    binding_keys: Dict[Tuple[str, Tuple[str, ...]], str] = {}
    scope_bindings: Dict[Tuple[int, str], Tuple[str, ...]] = {}
    synthetic_index = 0

    # A query-local CTE reference never needs its own catalog reflection.  If
    # a same-named physical leaf exists (the recursive-looking but valid
    # ``WITH x AS (SELECT ... FROM x)`` pattern), retain the historic/public
    # alias for that physical leaf and let the skipped CTE node share its
    # internal key.  This also keeps existing reflection/RBAC maps compatible.
    preferred_physical: Dict[
        str, Tuple[Tuple[str, ...], Tuple[str, str]]
    ] = {}
    for table in tables:
        if not table.name or id(table) in cte_references:
            continue
        alias = SQLParser._get_alias(table) or table.name
        db_name = SQLParser._get_db_name(table) or default_super_name
        preferred_physical.setdefault(
            alias.casefold(),
            (
                ("physical", db_name.casefold(), table.name.casefold()),
                (db_name, table.name),
            ),
        )

    for table in tables:
        table_name = table.name
        if not table_name:
            continue
        alias = SQLParser._get_alias(table) or table_name
        folded_alias = alias.casefold()
        db_name = SQLParser._get_db_name(table) or default_super_name
        is_cte_reference = id(table) in cte_references
        if is_cte_reference:
            relation: Tuple[str, ...] = ("derived", table_name.casefold())
        else:
            relation = (
                "physical",
                db_name.casefold(),
                table_name.casefold(),
            )

        scope_index = table_scope.get(id(table), -1)
        scoped_alias = (scope_index, folded_alias)
        prior_relation = scope_bindings.get(scoped_alias)
        if prior_relation is not None and prior_relation != relation:
            raise ValueError(
                f"Table alias {alias!r} resolves to multiple relations "
                "within one SQL scope"
            )
        scope_bindings[scoped_alias] = relation

        key_relation = (
            preferred_physical[folded_alias][0]
            if is_cte_reference and folded_alias in preferred_physical
            else relation
        )
        identity = (folded_alias, key_relation)
        key = binding_keys.get(identity)
        if key is None:
            if folded_alias not in used_key_folds:
                key = alias
            else:
                while True:
                    synthetic_index += 1
                    candidate = (
                        f"__supertable_binding_{synthetic_index}_{alias}"
                    )
                    candidate_fold = candidate.casefold()
                    if (
                        candidate_fold not in reserved_folds
                        and candidate_fold not in used_key_folds
                    ):
                        key = candidate
                        break
            binding_keys[identity] = key
            alias_to_table[key] = (
                preferred_physical[folded_alias][1]
                if is_cte_reference and folded_alias in preferred_physical
                else (db_name, table_name)
            )
            used_key_folds.add(key.casefold())

        by_node_id[id(table)] = key
        if not is_cte_reference:
            physical_keys.add(key)

    return _ScopedTableBindings(
        by_node_id=by_node_id,
        alias_to_table=alias_to_table,
        cte_reference_node_ids=cte_references,
        physical_keys=physical_keys,
    )


# User queries execute in backend sessions that also own managed reflection
# views.  A read-only SELECT is therefore not, by itself, a sandbox: DuckDB
# exposes connection settings, secret catalogs, files, extension entry points,
# and environment-dependent helpers as functions.  Keep the public SQL surface
# explicit and fail closed when sqlglot learns about (or an extension adds) a
# new function.
_COMMON_READ_FUNCTIONS = frozenset({
    # Boolean/conditional/type expressions.
    "and", "case", "cast", "coalesce", "decode", "exists", "if",
    "nullif", "nvl2", "or", "try", "try_cast", "typeof",
    # Numeric expressions and aggregates.
    "abs", "any_value", "approx_distinct", "approx_quantile", "avg", "cbrt",
    "ceil", "corr", "count", "count_if",
    "covar_pop", "covar_samp", "exp", "floor", "greatest", "least", "ln",
    "log", "max", "median", "min", "percentile_cont", "percentile_disc",
    "power", "quantile", "rand", "randn", "round", "safe_divide", "sign",
    "sqrt", "stddev", "stddev_pop", "stddev_samp", "sum", "variance",
    "variance_pop",
    # String/binary expressions.
    "cast_to_str_type", "chr", "collate", "concat", "concat_ws",
    "contains", "encode", "ends_with", "from_base", "from_base64", "hex",
    "initcap", "is_ascii", "left", "length", "levenshtein", "lower",
    "lower_hex", "md5", "md5_digest", "normalize", "overlay",
    "regexp_extract", "regexp_extract_all", "regexp_ilike", "regexp_like",
    "regexp_replace", "regexp_split", "replace", "right", "sha", "sha2",
    "split", "split_part", "starts_with", "str_position", "string",
    "string_to_array", "stuff", "substring", "substring_index", "to_base64",
    "to_char", "trim", "unhex", "unicode", "upper",
    # Temporal expressions.  Current clock values are data, not session
    # identity; connection/catalog identity functions are intentionally absent.
    "add_months", "current_date", "current_datetime", "current_time",
    "current_timestamp", "current_timestamp_l_t_z", "date", "datediff",
    "datetime", "datetime_add", "datetime_diff", "datetime_sub",
    "datetime_trunc", "date_add", "date_bin", "date_from_parts",
    "date_str_to_date", "date_sub", "date_to_date_str", "date_trunc", "day",
    "dayofweek_iso", "day_of_month", "day_of_week", "day_of_year", "extract",
    "from_iso8601_timestamp", "last_day", "make_interval", "month",
    "months_between", "quarter", "str_to_date", "str_to_time", "str_to_unix",
    "time", "timestamp", "timestampdiff", "timestamp_add",
    "timestamp_from_parts", "timestamp_sub", "timestamp_trunc", "time_add",
    "time_diff", "time_from_parts", "time_str_to_date", "time_str_to_time",
    "time_str_to_unix", "time_sub", "time_to_str", "time_to_time_str",
    "time_to_unix", "time_trunc", "to_days", "ts_or_di_to_di",
    "ts_or_ds_add", "ts_or_ds_diff", "ts_or_ds_to_date",
    "ts_or_ds_to_datetime", "ts_or_ds_to_date_str", "ts_or_ds_to_time",
    "ts_or_ds_to_timestamp", "unix_date", "unix_seconds", "unix_to_str",
    "unix_to_time", "unix_to_time_str", "week", "week_of_year", "year",
    # Bounded value-container and JSON operations.  Row generators, dynamic
    # table functions, XML/JSON table functions, and external readers are not
    # part of this list.
    "array", "array_all", "array_any", "array_concat",
    "array_contains", "array_contains_all", "array_first", "array_intersect",
    "array_last", "array_overlaps", "array_remove", "array_reverse",
    "array_size", "array_slice", "array_sort", "array_sum", "array_to_string",
    "json_array_contains", "json_extract", "json_extract_array",
    "json_extract_scalar", "json_format", "jsonb_contains", "jsonb_exists",
    "jsonb_extract", "jsonb_extract_scalar", "j_s_o_n_array", "j_s_o_n_cast",
    "j_s_o_n_exists", "j_s_o_n_object", "j_s_o_n_value_array", "map",
    "map_from_entries", "object_insert", "parse_json", "struct",
    "struct_extract", "to_array", "to_double", "to_map", "to_number",
    # Analytic aggregates/window functions.
    "first", "first_value", "lag", "last", "last_value", "lead",
    "logical_and", "logical_or", "nth_value", "row_number", "var_map", "xor",
})


# DuckDB functions that sqlglot intentionally represents as Anonymous.  They
# receive the same closed-list treatment as modelled functions.  In particular,
# current_setting, getvariable, duckdb_*, pragma_*, read_*, glob,
# input_file_name, query/query_table, getenv, and extension/UDF names are absent.
_DUCKDB_READ_FUNCTIONS = frozenset({
    "arbitrary", "ascii", "bit_and", "bit_count", "bit_length", "bit_or", "bit_xor",
    "bool_and", "bool_or", "date_diff", "date_part", "dayofmonth",
    "dayofweek", "dayofyear", "dense_rank", "entropy", "epoch", "epoch_ms",
    "epoch_ns", "epoch_us", "even", "favg", "fsum",
    "gamma", "geometric_mean", "hash", "ifnull", "instr", "isfinite",
    "kurtosis", "kurtosis_pop", "lcase", "lgamma", "mad",
    "make_date", "make_time", "make_timestamp", "make_timestamptz", "mode",
    "nvl", "ntile", "percent_rank", "product", "rank",
    "regr_avgx", "regr_avgy", "regr_count", "regr_intercept", "regr_r2",
    "regr_slope", "regr_sxx", "regr_sxy", "regr_syy", "regexp_escape",
    "regexp_full_match", "regexp_matches", "reverse", "sem",
    "signbit", "skewness", "stddev_pop", "stddev_samp",
    "strftime", "strip_accents", "strpos", "strptime", "time_bucket",
    "translate", "trunc", "ucase", "var_pop", "var_samp", "weekofyear",
    "weighted_avg", "yearweek",
})


_READ_FUNCTIONS_BY_DIALECT = {
    "duckdb": _COMMON_READ_FUNCTIONS | _DUCKDB_READ_FUNCTIONS,
    # Spark execution performs a second validation with its own backend list.
    # Keeping the parser baseline explicit also makes direct Spark parsing fail
    # closed until that backend opts a function in.
    "spark": _COMMON_READ_FUNCTIONS,
}

# SQLGlot 26.x represents these DuckDB session/catalog expressions as plain
# unqualified ``Column`` nodes rather than ``Func`` nodes.  DuckDB nevertheless
# evaluates them as connection metadata, so the function allowlist alone does
# not cover them.  Match only the exact, unquoted and unqualified spelling:
# ``records.user`` and ``"USER"`` remain ordinary data-column references.
_BARE_SESSION_IDENTIFIERS_BY_DIALECT = {
    "duckdb": frozenset({
        "user",
        "session_user",
        "current_role",
        "current_catalog",
        "current_schema",
    }),
}

_COLLECTION_AGGREGATE_FUNCTIONS = frozenset({
    "array_agg", "array_concat_agg", "array_union_agg", "array_unique_agg",
    "group_concat", "j_s_o_n_array_agg", "j_s_o_n_b_object_agg",
    "j_s_o_n_object_agg", "list", "string_agg",
})
_MAX_COLLECTION_AGGREGATE_SOURCE_ROWS = 10


def _normalized_function_name(node: exp.Func) -> str:
    if isinstance(node, (exp.Anonymous, exp.AnonymousAggFunc)):
        return str(node.name or "").strip().casefold()
    return str(node.sql_name() or "").strip().casefold()


def _has_bounded_collection_source(node: exp.Func) -> bool:
    """Require collection aggregates to consume an explicitly capped subquery."""
    owner = node.parent
    while owner is not None and not isinstance(owner, exp.Select):
        owner = owner.parent
    if not isinstance(owner, exp.Select):
        return False
    from_clause = owner.args.get("from")
    source = getattr(from_clause, "this", None)
    if not isinstance(source, exp.Subquery) or not isinstance(source.this, exp.Select):
        return False
    limit = source.this.args.get("limit")
    if limit is None or any(
        value is not None
        for key, value in limit.args.items()
        if key != "expression"
    ):
        return False
    limit_expression = getattr(limit, "expression", None)
    if isinstance(limit_expression, exp.Literal) and not limit_expression.is_string:
        try:
            value = int(str(limit_expression.this))
        except (TypeError, ValueError, OverflowError):
            return False
        return 0 <= value <= _MAX_COLLECTION_AGGREGATE_SOURCE_ROWS

    return False


def validate_read_query_functions(
    parsed: exp.Expression,
    dialect: str,
    *,
    allow_bounded_collection_aggregates: bool = False,
) -> None:
    """Reject every backend function outside the read-query allowlist.

    This validation is deliberately based on the parsed AST, not SQL text, so
    quoting, comments, qualification, nesting, CTEs, and window clauses cannot
    hide a call.  Backends that can be selected after initial parsing (AUTO)
    must call this helper again immediately before execution with their actual
    dialect.
    """
    allowed = _READ_FUNCTIONS_BY_DIALECT.get(str(dialect).casefold())
    if allowed is None:
        raise ValueError(f"Unsupported SQL dialect for read query: {dialect!r}")

    for node in parsed.find_all(exp.Func):
        name = _normalized_function_name(node)
        # ``evil.sum(x)`` parses as Dot(Identifier, Anonymous('sum')).  A
        # name-only check would mistake a namespaced UDF/extension function for
        # the trusted built-in of the same name.
        qualified = isinstance(node.parent, exp.Dot)
        collection_allowed = (
            allow_bounded_collection_aggregates
            and
            name in _COLLECTION_AGGREGATE_FUNCTIONS
            and _has_bounded_collection_source(node)
        )
        fixed_width_arg_extreme = (
            name in {"arg_max", "arg_min"}
            and node.args.get("count") is None
        )
        if qualified or not name or not (
            name in allowed or collection_allowed or fixed_width_arg_extreme
        ):
            safe_name = name or type(node).__name__.casefold()
            raise ValueError(
                f"SQL function '{safe_name}' is not allowed in supertable "
                "read queries"
            )


def _reject_unmanaged_table_sources(
    parsed: exp.Expression,
    dialect: str,
) -> None:
    """Require every physical FROM source to be a rewritable identifier."""
    for table in parsed.find_all(exp.Table):
        if table.args.get("version") is not None or table.args.get("when") is not None:
            raise ValueError(
                "VERSION/TIMESTAMP AS OF is not supported until historical "
                "supertable snapshots and deletion vectors can be pinned together"
            )
        source = table.this
        if not isinstance(source, exp.Identifier):
            rendered = table.sql(dialect=dialect)
            raise ValueError(
                "External or table-valued sources are not allowed in "
                f"supertable SELECT queries: {rendered}"
            )
        name = str(source.this or "")
        lowered = name.lower()
        if source.args.get("quoted") and (
            "/" in name
            or "\\" in name
            or lowered.startswith(("s3:", "s3a:", "http:", "https:"))
            or lowered.endswith((".parquet", ".csv", ".json"))
        ):
            raise ValueError(
                "External file sources are not allowed in supertable "
                f"SELECT queries: {table.sql(dialect=dialect)}"
            )


def _validate_schema_dependent_joins(parsed: exp.Expression) -> None:
    """Reject joins whose keys cannot be preserved before schema loading.

    NATURAL JOIN derives its keys from the runtime schema, so column
    projection or an RBAC view can silently change it into a different join.
    USING keys are explicit and safe only for the currently supported direct
    two-source shape; more complex accumulated-left bindings need schema-aware
    resolution before they can be projected and authorized correctly.
    """
    for select_expr in parsed.find_all(exp.Select):
        joins = list(select_expr.args.get("joins") or [])
        if any(
            str(join.args.get("method") or "").casefold() == "natural"
            for join in joins
        ):
            raise ValueError(
                "NATURAL JOIN is not supported in supertable read queries; "
                "use an explicit JOIN ... ON condition"
            )

        using_joins = [join for join in joins if join.args.get("using")]
        if not using_joins:
            continue
        from_clause = select_expr.args.get("from")
        left_source = getattr(from_clause, "this", None)
        right_source = using_joins[0].this
        if (
            len(joins) != 1
            or len(using_joins) != 1
            or not isinstance(left_source, exp.Table)
            or not isinstance(right_source, exp.Table)
        ):
            raise ValueError(
                "JOIN ... USING is supported only between two direct table "
                "sources in supertable read queries; use an explicit "
                "JOIN ... ON condition"
            )


def _reject_bare_session_identifiers(
    parsed: exp.Expression,
    dialect: str,
) -> None:
    """Reject backend identity expressions parsed as ordinary columns."""
    denied = _BARE_SESSION_IDENTIFIERS_BY_DIALECT.get(
        str(dialect).casefold(), frozenset()
    )
    if not denied:
        return
    for column in parsed.find_all(exp.Column):
        identifier = column.this
        if (
            column.table
            or column.db
            or column.catalog
            or not isinstance(identifier, exp.Identifier)
            or bool(identifier.args.get("quoted"))
        ):
            continue
        name = str(identifier.this or "").strip().casefold()
        if name in denied:
            raise ValueError(
                f"DuckDB session identity expression '{name}' is not allowed "
                "in supertable read queries; quote or qualify it to read a "
                "data column with that name"
            )


def validate_read_query_ast(
    parsed: exp.Expression,
    dialect: str,
    *,
    allow_bounded_collection_aggregates: bool = False,
) -> None:
    """Validate the complete read AST at parser and backend boundaries."""
    if not isinstance(parsed, exp.Query):
        raise ValueError(
            "Only read-only SELECT/WITH/set-operation queries are allowed "
            "on the supertable read path"
        )
    mutating_types = tuple(
        expression_type
        for name in (
            "Insert", "Update", "Delete", "Merge", "Create", "Drop",
            "Alter", "Command", "Copy", "Transaction", "Commit", "Rollback",
            "Grant", "Revoke", "TruncateTable", "Into", "Lock",
        )
        if isinstance((expression_type := getattr(exp, name, None)), type)
    )
    if mutating_types and any(
        isinstance(node, mutating_types) for node in parsed.walk()
    ):
        raise ValueError(
            "Only read-only SELECT/WITH/set-operation queries are allowed "
            "on the supertable read path"
        )
    _validate_schema_dependent_joins(parsed)
    _reject_unmanaged_table_sources(parsed, dialect)
    _reject_bare_session_identifiers(parsed, dialect)
    validate_read_query_functions(
        parsed,
        dialect,
        allow_bounded_collection_aggregates=(
            allow_bounded_collection_aggregates
        ),
    )


# ---------------------------------------------------------------------------
# Predicate → interval extraction helpers (read-path file pruning)
# ---------------------------------------------------------------------------

_COMPARISON_OPS: Dict[type, str] = {
    exp.EQ: "eq",
    exp.GT: "gt",
    exp.GTE: "gte",
    exp.LT: "lt",
    exp.LTE: "lte",
}

# When the column sits on the RHS (``5 < t.x``) the operator flips.
_FLIP_OP: Dict[str, str] = {
    "eq": "eq", "gt": "lt", "gte": "lte", "lt": "gt", "lte": "gte",
}


def _unwrap_paren(node: exp.Expression) -> exp.Expression:
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _split_and(node: exp.Expression) -> List[exp.Expression]:
    """Flatten a top-level conjunction into its AND-connected leaves.

    Only ``AND`` is split; an ``OR`` (or anything else) is returned whole so the
    caller treats it as a single, un-extractable predicate (→ no constraint).
    """
    node = _unwrap_paren(node)
    if isinstance(node, exp.And):
        return _split_and(node.left) + _split_and(node.right)
    return [node]


def _parse_datetime_literal(
        s: str,
        *,
        timezone_mode: str = "naive",
) -> Optional[datetime]:
    """Parse an ISO-ish temporal literal into the stats representation.

    Parquet stores zoned timestamp bounds as UTC instants with the timezone
    removed by :func:`supertable.processing._to_us_datetime`.  An explicit
    offset can therefore be normalised safely.  A TIMESTAMPTZ literal without
    an offset depends on the executor session timezone, which the parser does
    not know, so it deliberately yields ``None`` (no pruning).  Conversely an
    offset-bearing value cannot be compared as a naïve TIMESTAMP/DATE.
    """
    txt = s.strip().replace("T", " ")
    if txt.endswith(("Z", "z")):
        txt = txt[:-1] + "+00:00"
    try:
        value = datetime.fromisoformat(txt)
    except ValueError:
        return None

    if timezone_mode == "aware":
        if value.tzinfo is None:
            return None
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    if value.tzinfo is not None:
        return None
    return value


def _literal_to_lane_value(node: exp.Expression) -> Optional[Tuple[str, object]]:
    """Reduce a literal-ish expression to ``(lane, value)`` or ``None``.

    Lanes: ``numeric`` (bare ints/floats/bools), ``numeric_cast`` (a numeric
    value whose CAST provenance matters to executor overflow rules), ``string``
    (quoted strings), ``date``, ``timestamp`` and ``timestamptz``.  Any expression
    that isn't a pure literal (a column, a function, an arithmetic node, a
    subquery) yields ``None`` → that predicate contributes no constraint.
    """
    node = _unwrap_paren(node)

    if isinstance(node, exp.Neg):
        inner = _literal_to_lane_value(node.this)
        if inner is not None and inner[0] == "numeric":
            return "numeric", -inner[1]
        return None

    if isinstance(node, exp.Boolean):
        return "numeric", 1 if node.this else 0

    if isinstance(node, (exp.Cast, exp.TryCast)):
        to = node.args.get("to")
        type_name = ""
        if to is not None and getattr(to, "this", None) is not None:
            type_value = getattr(to.this, "value", to.this)
            type_name = str(type_value).upper()
        inner = node.this
        temporal_lane = None
        timezone_mode = "naive"
        if type_name == "DATE":
            temporal_lane = "date"
        elif type_name in ("TIMESTAMPTZ", "TIMESTAMP_TZ") or (
                "TIMESTAMP" in type_name and "TIME ZONE" in type_name):
            temporal_lane = "timestamptz"
            timezone_mode = "aware"
        elif "TIMESTAMP" in type_name or "DATETIME" in type_name:
            temporal_lane = "timestamp"
        if temporal_lane is not None:
            # Resolution-changing casts cannot use the literal text's parsed
            # microsecond value as their comparison bound.  DuckDB rounds, for
            # example, TIMESTAMP_MS '...00.123456' to ...00.123000 and
            # TIMESTAMP_S to the nearest second.  A parameterized TIMESTAMP(3)
            # does the same.  TIMESTAMP_NS has the opposite problem: Python's
            # datetime parser silently truncates fractional digits beyond
            # microseconds, so e.g. a strict bound at ``...1234567`` cannot be
            # represented exactly.  Reproducing every executor's precision
            # conversion (including rollover and pre-epoch cases) is
            # unnecessary risk for an optional optimisation, so reject every
            # explicitly resolution-qualified/parameterized form.
            temporal_params = to.args.get("expressions") or []
            if type_name in {
                "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS",
            } or temporal_params:
                return None
            if isinstance(inner, exp.Literal) and inner.is_string:
                dt = _parse_datetime_literal(
                    inner.this, timezone_mode=timezone_mode)
                if temporal_lane == "date" and dt is not None:
                    # DuckDB accepts a datetime-shaped DATE literal but
                    # truncates it to the calendar date.  Mirror that exact
                    # value; retaining the time-of-day could exclude the file
                    # containing midnight on an equality predicate.
                    dt = datetime(dt.year, dt.month, dt.day)
                return (temporal_lane, dt) if dt is not None else None
            return None
        inner_lv = _literal_to_lane_value(inner)
        if inner_lv is None:
            return None
        lane, value = inner_lv
        # The engine compares in the CAST's target lane, so the constraint must
        # live there too — falling through to the inner literal's lane would
        # e.g. turn CAST('1.5' AS DOUBLE) into a STRING constraint whose
        # byte-order pruning excludes a numerically matching '1.50'.
        if any(t in type_name for t in ("DOUBLE", "FLOAT", "REAL")):
            # A floating cast can round an integral-looking source before the
            # executor compares it to a BIGINT column.  Keeping the original
            # Python int here is unsound past 2**53; the SELECT-safe stats path
            # does not prune floating lanes anyway, so fail open immediately.
            return None
        if any(t in type_name for t in ("INT", "DECIMAL", "NUMERIC")):
            if lane == "numeric":
                return "numeric_cast", value
            if lane == "numeric_cast":
                return inner_lv
            if lane == "string":
                try:
                    text = value.strip()
                    return "numeric_cast", (float(text) if any(
                        c in text for c in (".", "e", "E")) else int(text))
                except (TypeError, ValueError, AttributeError):
                    return None
            return None
        if any(t in type_name for t in ("CHAR", "TEXT", "STRING")):
            # A numeric rendered as text is dialect-formatting territory
            # ('1.50' vs '1.5') — only pass through genuine string literals.
            return inner_lv if lane == "string" else None
        return None  # unknown target type → no constraint (retain)

    if isinstance(node, exp.Literal):
        if node.is_string:
            return "string", node.this
        text = node.this
        try:
            if any(c in text for c in (".", "e", "E")):
                return "numeric", float(text)
            return "numeric", int(text)
        except (TypeError, ValueError):
            return None

    return None


def _interval_for_op(op: str, lane: str, value: object) -> PredInterval:
    if op == "eq":
        return PredInterval(lane, value, True, value, True)
    if op == "gt":
        return PredInterval(lane, value, False, None, True)
    if op == "gte":
        return PredInterval(lane, value, True, None, True)
    if op == "lt":
        return PredInterval(lane, None, True, value, False)
    # lte
    return PredInterval(lane, None, True, value, True)


def _max_lower(alo, ai, blo, bi):
    """Tighter (greater) of two lower bounds; ``None`` == -inf."""
    if alo is None:
        return blo, bi
    if blo is None:
        return alo, ai
    if alo > blo:
        return alo, ai
    if alo < blo:
        return blo, bi
    return alo, (ai and bi)


def _min_upper(ahi, ai, bhi, bi):
    """Tighter (lesser) of two upper bounds; ``None`` == +inf."""
    if ahi is None:
        return bhi, bi
    if bhi is None:
        return ahi, ai
    if ahi < bhi:
        return ahi, ai
    if ahi > bhi:
        return bhi, bi
    return ahi, (ai and bi)


def _intersect_intervals(a: PredInterval, b: PredInterval) -> Optional[PredInterval]:
    """Intersect two predicates on the same column; ``None`` if their lanes
    conflict (the column then becomes un-prunable)."""
    if a.lane != b.lane:
        return None
    lo, lo_incl = _max_lower(a.lo, a.lo_incl, b.lo, b.lo_incl)
    hi, hi_incl = _min_upper(a.hi, a.hi_incl, b.hi, b.hi_incl)
    return PredInterval(a.lane, lo, lo_incl, hi, hi_incl)


class SQLParser:
    """
    Minimal SQL parser for extracting table/column mappings.

    Input:
        SQLParser(super_name: str, query: str, dialect: str)

    Supported dialects: "duckdb", "spark"

    Output:
        get_table_tuples() -> List[TableDefinition]

    Each TableDefinition corresponds to:
        (super_name, simple_name, alias, columns)

    Where:
        - super_name: schema/namespace.
          If missing in SQL, the provided `super_name` argument is used.
        - simple_name: the table name (without schema).
        - alias: the table alias used in the query.
          If no alias is defined, alias = table name.
        - columns: List[str]
            - Each item is the referenced physical column name.
            - Aliases in SELECT (e.g. "o.id AS order_id") do NOT appear;
              only "id" is recorded.
            - Per-table column list is:
                - de-duplicated by column name
                - sorted deterministically (lexicographically).
            - Special semantics:
                - If SELECT * is present:
                    - We store [] for every table alias,
                      meaning "all columns for this table".
                - If SELECT t.* is present:
                    - We store [] for alias t,
                      meaning "all columns for that table".

    Rules / behavior:
        - Qualified columns (t_alias.col):
            - Resolved via the nearest SELECT scope's FROM/JOIN bindings.
            - Correlated references may resolve through parent scopes.
            - A CTE/derived binding is authoritative and is never rebound to
              a same-named physical alias in another scope.
        - Unqualified columns:
            - If there is exactly one direct physical source in the current
              SELECT scope, they are attributed to that source.
            - If the current scope has multiple sources, every direct physical
              source is conservatively kept at full projection because only
              the runtime schemas can resolve the name.
            - Derived sources retain the independently collected dependencies
              of their physical leaf scopes.
        - For SELECT projections with aliases, e.g. "o.id AS order_id":
            - We record "id" for alias "o".
        - Star handling:
            - SELECT *       -> all aliases: []
            - SELECT t.*     -> alias t: []
            - Never record "*" as a physical column name.
        - We do not record columns for non-Column expressions.
    """

    SUPPORTED_DIALECTS = ("duckdb", "spark")

    def __init__(
        self,
        super_name: str,
        query: str,
        dialect: str,
        *,
        allow_bounded_collection_aggregates: bool = False,
    ):
        if not super_name or not isinstance(super_name, str):
            raise ValueError("Parameter 'super_name' must be a non-empty string.")

        if not query or not isinstance(query, str):
            raise ValueError("Parameter 'query' must be a non-empty SQL string.")

        if dialect not in self.SUPPORTED_DIALECTS:
            raise ValueError(
                f"Parameter 'dialect' must be one of {self.SUPPORTED_DIALECTS}, got '{dialect}'."
            )

        self.default_super_name: str = super_name
        self.original_query: str = query
        self.dialect: str = dialect
        self.allow_bounded_collection_aggregates = bool(
            allow_bounded_collection_aggregates
        )

        # Internal parsed expression
        self._parsed: exp.Expression = self._parse_query(query, dialect)
        validate_read_query_ast(
            self._parsed,
            self.dialect,
            allow_bounded_collection_aggregates=(
                self.allow_bounded_collection_aggregates
            ),
        )

        # Predicate and join pruning analyses consume the same sqlglot scope
        # graph and are strictly read-only.  Build it lazily and retain an
        # immutable outer container so a normal parser user pays nothing, while
        # DataReader avoids traversing every CTE/subquery twice.
        self._pruning_scopes: Optional[Tuple[object, ...]] = None

        # alias -> (supertable, table)
        self._alias_to_table: Dict[str, Tuple[str, str]] = {}
        # SQL identifiers are case-insensitive.  Retain one canonical spelling
        # when the same physical relation is referenced with case-only alias
        # variants in independent scopes (for example UNION branches).
        self._canonical_alias_by_fold: Dict[str, str] = {}
        # alias -> unqualified names whose binding depends on the runtime input
        # schema.  This includes SELECT-alias collisions, unresolved names in
        # multi/correlated scopes, and relation-alias whole-row expressions.
        # Projection must retain candidate schemas, and an RBAC column policy
        # must not remove a precedence-winning physical column and thereby
        # rebind the same token to an alias/struct.  The historical GROUP name
        # remains as an API alias for older executor integrations.
        self._binding_ambiguities: Dict[str, Set[str]] = {}
        self._group_alias_ambiguities = self._binding_ambiguities

        # alias -> ordered unique list of column names
        # (or [] if meaning "all columns" due to * or t.*)
        self._alias_to_columns: Dict[str, List[str]] = {}
        self._cte_reference_node_ids: Set[int] = set()
        self._table_binding_by_node_id: Dict[int, str] = {}
        self._physical_aliases: Set[str] = set()

        self._extract_tables()
        self._cte_names: Set[str] = self._collect_cte_names()
        self._extract_columns()

    def _reject_unmanaged_table_sources(self) -> None:
        """Reject table functions and path literals on the catalog read path.

        Executors enforce tombstones/RBAC by replacing catalog table nodes
        with managed views.  A source such as ``read_parquet('/path')`` is not
        such a node and would execute unchanged, bypassing both protections.
        Keep scalar/row-generating expressions available, but require every
        physical table source to be an identifier that can be resolved and
        rewritten by the catalog pipeline.
        """
        for table in self._parsed.find_all(exp.Table):
            if (
                table.args.get("version") is not None
                or table.args.get("when") is not None
            ):
                # The catalog currently pins only the current immutable
                # resource+tombstone snapshot.  Leaving an AS OF clause on the
                # rewritten temporary view would either error or ask the SQL
                # backend for unrelated history while applying current delete
                # state.  Reject until historical resources and their exact DV
                # can be resolved atomically together.
                raise ValueError(
                    "VERSION/TIMESTAMP AS OF is not supported until historical "
                    "supertable snapshots and deletion vectors can be pinned together"
                )
            source = table.this
            if not isinstance(source, exp.Identifier):
                rendered = table.sql(dialect=self.dialect)
                raise ValueError(
                    "External or table-valued sources are not allowed in "
                    f"supertable SELECT queries: {rendered}"
                )

            # DuckDB accepts a quoted filename directly in FROM.  Although it
            # parses as an Identifier, it is still an unmanaged external scan.
            name = str(source.this or "")
            lowered = name.lower()
            if source.args.get("quoted") and (
                "/" in name
                or "\\" in name
                or lowered.startswith(("s3:", "s3a:", "http:", "https:"))
                or lowered.endswith((".parquet", ".csv", ".json"))
            ):
                raise ValueError(
                    "External file sources are not allowed in supertable "
                    f"SELECT queries: {table.sql(dialect=self.dialect)}"
                )

    # ---------------- Parsing helpers ----------------

    @staticmethod
    def _build_parse_error_message(error: ParseError) -> str:
        """
        Build a concise, user-facing message from sqlglot.ParseError.
        """
        errors = getattr(error, "errors", None) or []
        if errors:
            err = errors[0]

            description = (err.get("description") or "").strip()
            if not description:
                raw_lines = str(error).strip().splitlines()
                first_line = raw_lines[0] if raw_lines else "Unknown parse error"
                description = first_line.rstrip(".")

            line = err.get("line")
            col = err.get("col")

            header = description
            if line is not None and col is not None:
                header = f"{header} Line {line}, Col: {col}."

            start = (err.get("start_context") or "")
            highlight = (err.get("highlight") or "")
            end = (err.get("end_context") or "")
            context = f"{start}{highlight}{end}".rstrip("\n").rstrip()

            if context:
                return f"{header}\n  {context}"

            return header or "Invalid SQL syntax."

        raw = str(error).strip()
        if not raw:
            return "Invalid SQL syntax."
        return raw.splitlines()[0]

    @staticmethod
    def _parse_query(query: str, dialect: str) -> exp.Expression:
        try:
            parsed = sqlglot.parse(query, read=dialect)
            semicolon_type = getattr(exp, "Semicolon", None)
            statements = [
                statement
                for statement in parsed
                if statement is not None
                and not (
                    isinstance(semicolon_type, type)
                    and isinstance(statement, semicolon_type)
                )
            ]
            if len(statements) != 1:
                raise ValueError("Exactly one SQL statement is required")
            return statements[0]
        except ParseError as e:
            message = SQLParser._build_parse_error_message(e)
            raise ValueError(f"Failed to parse SQL query: {message}") from None
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(
                f"An unexpected error occurred while parsing SQL query: {e}"
            ) from None

    @staticmethod
    def _get_alias(table_expr: exp.Table) -> str:
        """
        Return the alias of a table if present; otherwise the table name.
        """
        alias_expr = table_expr.args.get("alias")
        if isinstance(alias_expr, exp.TableAlias):
            ident = alias_expr.this
            if isinstance(ident, exp.Identifier):
                return ident.name
        return table_expr.name

    @staticmethod
    def _get_db_name(table_expr: exp.Table) -> Optional[str]:
        """
        Return the DB/schema (supertable) name if present.
        """
        db_expr = table_expr.args.get("db")
        if isinstance(db_expr, exp.Identifier):
            return db_expr.name
        if isinstance(db_expr, exp.Expression) and hasattr(db_expr, "name"):
            return db_expr.name
        return None

    def _get_pruning_scopes(self) -> Tuple[object, ...]:
        """Return the lazily built scope graph shared by pruning analyses.

        ``traverse_scope`` is eager in the supported sqlglot version, and both
        consumers only read each Scope/AST node.  Caching the tuple therefore
        cannot change bindings; it only removes a duplicate traversal.
        Exceptions deliberately propagate to each public method's existing
        fail-open guard, which turns analysis failure into no pruning.
        """
        if self._pruning_scopes is None:
            self._pruning_scopes = tuple(traverse_scope(self._parsed))
        return self._pruning_scopes

    def _collect_cte_reference_node_ids(self) -> Set[int]:
        """Return Table-node identities that sqlglot scope binds to a CTE.

        Name-only CTE detection is unsafe.  In ``WITH x AS (SELECT * FROM x)``
        DuckDB resolves the inner ``x`` to an existing catalog table while the
        outer ``x`` is the CTE.  Treating both nodes as the CTE would leave the
        physical source outside snapshot/RBAC rewriting.  The scope graph
        distinguishes them: a real source resolves to ``exp.Table`` and a CTE
        reference resolves to a child ``Scope``.
        """
        references: Set[int] = set()
        for scope in self._get_pruning_scopes():
            for selected in scope.selected_sources.values():
                try:
                    node, source = selected
                except (TypeError, ValueError):
                    continue
                if isinstance(node, exp.Table) and not isinstance(source, exp.Table):
                    references.add(id(node))
        return references

    # ---------------- Table extraction ----------------

    def _extract_tables(self) -> None:
        """
        Build alias -> (supertable, table) mapping.

        Rules:
            - If table has explicit schema (e.g. stock.orders), use that.
            - Otherwise, prefix with default supertable.
            - If no alias is present, alias = table name.
        """
        layout = _build_scoped_table_bindings(
            self._parsed,
            self.default_super_name,
            scopes=self._get_pruning_scopes(),
        )
        if not layout.alias_to_table:
            raise ValueError("No tables found in SQL query.")
        self._alias_to_table = layout.alias_to_table
        self._canonical_alias_by_fold = {
            alias.casefold(): alias for alias in layout.alias_to_table
        }
        self._cte_reference_node_ids = layout.cte_reference_node_ids
        self._table_binding_by_node_id = layout.by_node_id
        self._physical_aliases = layout.physical_keys

    # ---------------- CTE detection ────────────────────────────────── #

    def _collect_cte_names(self) -> Set[str]:
        """
        Return the set of CTE names defined in WITH clauses.

        These names are *not* physical tables — they are query-scoped
        aliases for subqueries.  ``get_physical_tables()`` uses this set
        to exclude CTE references from the physical table list.
        """
        names: Set[str] = set()
        for cte in self._parsed.find_all(exp.CTE):
            alias_expr = cte.args.get("alias")
            if isinstance(alias_expr, exp.TableAlias):
                ident = alias_expr.this
                if isinstance(ident, exp.Identifier) and ident.name:
                    names.add(ident.name)
        return names

    # ---------------- Column extraction helpers ----------------

    @staticmethod
    def _is_inside_window_spec(col: exp.Column) -> bool:
        """Whether *col* belongs to an OVER/WINDOW specification.

        DuckDB resolves unqualified partition/order names against physical
        inputs before SELECT aliases when both exist, but accepts the SELECT
        alias when the input is absent.  Columns used by the window function
        itself (for example ``SUM(x) OVER (...)``) are ordinary source
        expressions and must not be mistaken for specification references.
        On the ascent to ``Window``, ``child is window.this`` distinguishes
        the function expression from PARTITION/ORDER/frame arguments in both
        inline and named WINDOW forms.
        """
        child: exp.Expression = col
        node = col.parent
        while node is not None:
            if isinstance(node, exp.Window):
                return child is not node.this
            if isinstance(node, exp.Select):
                return False
            child = node
            node = node.parent
        return False

    @staticmethod
    def _is_inside_alias_scope(col: exp.Column) -> bool:
        """
        True if this Column lives inside a clause where SELECT alias
        references are legal in DuckDB: WHERE, GROUP BY, ORDER BY, HAVING,
        QUALIFY, DISTINCT ON, or a window specification. Some clauses prefer
        a same-named physical input while others prefer the alias; the caller
        handles that distinction separately.

        Walking up the AST from the Column node, if we hit one of these
        clause types before reaching the Select node, the column is in
        alias scope and may be a reference to a computed SELECT alias
        rather than a physical table column.
        """
        node = col.parent
        while node is not None:
            if isinstance(node, exp.Window):
                return SQLParser._is_inside_window_spec(col)
            if isinstance(
                node,
                (
                    exp.Where,
                    exp.Group,
                    exp.Order,
                    exp.Having,
                    exp.Qualify,
                    exp.Distinct,
                ),
            ):
                return True
            if isinstance(node, exp.Select):
                # Reached SELECT without passing through an alias-aware clause.
                return False
            node = node.parent
        return False

    @staticmethod
    def _is_inside_physical_alias_precedence_scope(col: exp.Column) -> bool:
        """Whether DuckDB may prefer a physical input over a SELECT alias."""
        if SQLParser._is_inside_window_spec(col):
            return True
        node = col.parent
        while node is not None:
            if isinstance(node, (exp.Where, exp.Group, exp.Qualify)):
                return True
            if isinstance(node, exp.Select):
                return False
            node = node.parent
        return False

    # ---------------- Column extraction ----------------

    def _extract_columns(self) -> None:
        """
        Populate self._alias_to_columns:

            alias -> sorted unique list of column names

        Special handling:
            - SELECT *:
                [] for every alias = all columns.
            - SELECT t.*:
                [] for alias t = all columns of that table.
            - Star semantics override any specific collected columns.
            - Never record "*" as a real column name.
        """
        alias_to_columns: Dict[str, List[str]] = {
            alias: [] for alias in self._alias_to_table
        }
        seen_per_alias: Dict[str, Set[str]] = {
            alias: set() for alias in self._alias_to_table
        }
        force_full_projection: Set[str] = set()

        # Bind columns through the nearest sqlglot Scope, never through a
        # query-global alias map.  The same spelling can denote a physical
        # table in one scope and a CTE/derived relation in another.  A global
        # lookup would therefore project derived names such as ``finite_value``
        # or ``freq`` from an unrelated Parquet leaf.
        scopes_by_select: Dict[int, object] = {}
        bindings_by_scope: Dict[int, Dict[str, Optional[str]]] = {}
        owner_by_select: Dict[int, str] = {}
        for scope in self._get_pruning_scopes():
            if not isinstance(scope.expression, exp.Select):
                continue
            scopes_by_select[id(scope.expression)] = scope
            bindings: Dict[str, Optional[str]] = {}
            for source_alias, selected in scope.selected_sources.items():
                try:
                    node, source = selected
                except (TypeError, IndexError, ValueError):
                    continue
                folded = str(source_alias).casefold()
                # A Scope source is a CTE or derived table.  Store the binding
                # explicitly as None so a same-named outer physical alias can
                # never capture its qualified columns.
                bindings[folded] = (
                    self._table_binding_by_node_id.get(id(node))
                    if isinstance(node, exp.Table)
                    and isinstance(source, exp.Table)
                    else None
                )
            bindings_by_scope[id(scope)] = bindings

            # Unqualified columns are attributable only when this scope has
            # exactly one source and that source is physical.  Counting only
            # physical sources is unsafe for a physical+derived join because
            # the unqualified name may belong to the derived relation.
            if len(bindings) == 1:
                sole_owner = next(iter(bindings.values()))
                if sole_owner is not None:
                    owner_by_select[id(scope.expression)] = sole_owner

        def _nearest_scope(column: exp.Column):
            nearest_select = column.find_ancestor(exp.Select)
            if nearest_select is None:
                return None
            return scopes_by_select.get(id(nearest_select))

        def _resolve_qualified(column: exp.Column) -> Optional[str]:
            """Resolve a qualifier locally, climbing only for correlation."""
            qualifier = column.table.casefold()
            scope = _nearest_scope(column)
            while scope is not None:
                bindings = bindings_by_scope.get(id(scope), {})
                if qualifier in bindings:
                    # None is an authoritative derived-source binding.  Do not
                    # climb and accidentally rebind it to an outer table.
                    return bindings[qualifier]
                scope = getattr(scope, "parent", None)
            return None

        def _resolve_column(column: exp.Column) -> Optional[str]:
            if column.table:
                return _resolve_qualified(column)
            nearest_select = column.find_ancestor(exp.Select)
            if nearest_select is None:
                return None
            return owner_by_select.get(id(nearest_select))

        # SELECT aliases are local to their Select.  Keep one alias set per
        # Select so an inner GROUP/ORDER alias cannot be confused with an
        # outer projection (or vice versa).
        select_alias_names: Dict[int, Set[str]] = {}
        select_alias_positions: Dict[int, Dict[str, int]] = {}
        projection_positions: Dict[int, int] = {}
        using_columns_by_owner: Dict[str, Set[str]] = {}
        table_star_aliases: Set[str] = set()
        for scope in self._get_pruning_scopes():
            select_expr = scope.expression
            if not isinstance(select_expr, exp.Select):
                continue
            projection_aliases: Set[str] = set()
            alias_positions: Dict[str, int] = {}
            for projection_index, proj in enumerate(select_expr.expressions):
                projection_positions[id(proj)] = projection_index
                if isinstance(proj, exp.Alias):
                    alias_ident = proj.args.get("alias")
                    if isinstance(alias_ident, exp.Identifier) and alias_ident.name:
                        folded_alias = alias_ident.name.casefold()
                        projection_aliases.add(folded_alias)
                        alias_positions.setdefault(folded_alias, projection_index)

                # Case 1: explicit Star node
                if isinstance(proj, exp.Star):
                    # A bare star expands every direct source in this scope.
                    # Derived sources already contribute their leaf columns in
                    # their own scopes; direct physical sources require all.
                    if proj.this is None:
                        for owner in bindings_by_scope.get(id(scope), {}).values():
                            if owner is not None:
                                table_star_aliases.add(owner)
                    else:
                        table_ref = proj.this
                        qualifier: Optional[str] = None
                        if isinstance(table_ref, exp.Identifier):
                            qualifier = table_ref.name
                        elif hasattr(table_ref, "name"):
                            qualifier = table_ref.name
                        if qualifier:
                            owner = bindings_by_scope.get(id(scope), {}).get(
                                qualifier.casefold()
                            )
                            if owner is not None:
                                table_star_aliases.add(owner)

                # Case 2: some sqlglot versions may represent t.* as Column(name="*", table="t")
                elif isinstance(proj, exp.Column) and proj.name == "*":
                    owner = _resolve_column(proj)
                    if owner is not None:
                        table_star_aliases.add(owner)
            select_alias_names[id(select_expr)] = projection_aliases
            select_alias_positions[id(select_expr)] = alias_positions

            # Validation above admits USING only for one direct two-source
            # join.  Its identifiers are not exp.Column nodes, so record them
            # explicitly on both physical operands.  This is required not just
            # for loading: RBAC must see the join key as a requested column and
            # reject an excluded key before a restricted view can alter the
            # join's semantics.
            joins = list(select_expr.args.get("joins") or [])
            if len(joins) == 1 and joins[0].args.get("using"):
                from_clause = select_expr.args.get("from")
                left_source = getattr(from_clause, "this", None)
                right_source = joins[0].this
                if isinstance(left_source, exp.Table) and isinstance(
                    right_source, exp.Table
                ):
                    owners = (
                        self._table_binding_by_node_id.get(id(left_source)),
                        self._table_binding_by_node_id.get(id(right_source)),
                    )
                    using_names = {
                        identifier.name
                        for identifier in joins[0].args.get("using") or []
                        if isinstance(identifier, exp.Identifier)
                        and identifier.name
                    }
                    for owner in owners:
                        if owner in self._physical_aliases:
                            using_columns_by_owner.setdefault(
                                owner, set(),
                            ).update(using_names)

        def _projection_position(
            column: exp.Column,
            select_expr: Optional[exp.Select],
        ) -> Optional[int]:
            """Return the containing SELECT-list position, if any."""
            if select_expr is None:
                return None
            child: exp.Expression = column
            node = column.parent
            while node is not None and node is not select_expr:
                child = node
                node = node.parent
            if node is not select_expr:
                return None
            return projection_positions.get(id(child))

        def _force_full_for_scope(
            scope: object,
            *,
            include_ancestors: bool = False,
        ) -> None:
            """Retain physical schemas for bindings the AST cannot prove."""
            current_scope: Optional[object] = scope
            while current_scope is not None:
                force_full_projection.update(
                    owner
                    for owner in bindings_by_scope.get(
                        id(current_scope), {}
                    ).values()
                    if owner is not None
                )
                if not include_ancestors:
                    break
                current_scope = getattr(current_scope, "parent", None)

        def _record_schema_dependent_binding(
            scope: object,
            column_name: str,
            *,
            include_ancestors: bool = False,
        ) -> None:
            """Record every physical binding that may own an unqualified name."""
            current_scope: Optional[object] = scope
            while current_scope is not None:
                for owner in bindings_by_scope.get(
                    id(current_scope), {}
                ).values():
                    if owner is not None:
                        self._binding_ambiguities.setdefault(
                            owner, set(),
                        ).add(column_name)
                if not include_ancestors:
                    break
                current_scope = getattr(current_scope, "parent", None)

        # Attribute every non-star column exactly once.  Direct Alias values
        # are normal source expressions and are intentionally included; only a
        # reference to the alias from an alias-aware clause is suppressed.
        for col in self._parsed.find_all(exp.Column):
            col_name = col.name
            if not col_name or col_name == "*":
                continue

            nearest_select = col.find_ancestor(exp.Select)
            local_aliases = (
                select_alias_names.get(id(nearest_select), set())
                if nearest_select is not None
                else set()
            )
            folded_column = col_name.casefold()
            projection_position = _projection_position(col, nearest_select)
            alias_position = (
                select_alias_positions.get(id(nearest_select), {}).get(
                    folded_column
                )
                if nearest_select is not None
                else None
            )
            references_prior_projection_alias = (
                projection_position is not None
                and alias_position is not None
                and alias_position < projection_position
            )
            if (
                folded_column in local_aliases
                and not col.table
                and (
                    self._is_inside_alias_scope(col)
                    or references_prior_projection_alias
                )
            ):
                if (
                    self._is_inside_physical_alias_precedence_scope(col)
                    or references_prior_projection_alias
                ):
                    # DuckDB resolves unqualified names in WHERE, GROUP BY and
                    # QUALIFY, window specifications, and later SELECT-list
                    # expressions to an input column before a SELECT alias
                    # when both exist, while also accepting the alias when the
                    # input is absent. The parser has no physical schema yet,
                    # so deciding either way can change results or reject a
                    # valid query after projection pruning.
                    # Mark every direct physical source in this scope so the
                    # executor can disable projection pruning without turning
                    # the alias spelling itself into a literal Parquet column.
                    scope = _nearest_scope(col)
                    if scope is not None:
                        _force_full_for_scope(scope)
                        _record_schema_dependent_binding(scope, col_name)
                continue

            scope = _nearest_scope(col)
            scope_bindings = (
                bindings_by_scope.get(id(scope), {})
                if scope is not None
                else {}
            )
            if (
                not col.table
                and len(scope_bindings) == 1
                and folded_column in scope_bindings
                and scope_bindings[folded_column] is not None
            ):
                # DuckDB treats a bare relation alias as a whole-row struct
                # when no same-named physical column exists; that physical
                # column takes precedence when present. Keeping only a guessed
                # column loses one of those valid bindings, so retain the
                # relation's complete visible schema.
                _force_full_for_scope(scope)
                _record_schema_dependent_binding(scope, col_name)
                continue

            if (
                not col.table
                and scope is not None
                and bool(getattr(scope, "can_be_correlated", False))
            ):
                # SQL resolves an unqualified name locally and then through
                # correlated ancestor scopes if no local source exposes it.
                # Without runtime schemas, assigning it to either side can
                # request a nonexistent local column or project away the outer
                # column that actually binds. Preserve every physical
                # candidate and let the backend perform its normal bind.
                _force_full_for_scope(scope, include_ancestors=True)
                _record_schema_dependent_binding(
                    scope,
                    col_name,
                    include_ancestors=True,
                )
                continue

            resolved_alias = _resolve_column(col)
            if not resolved_alias:
                # An unqualified name in a multi-source scope cannot be
                # attributed without the runtime schemas. Explicitly retain
                # every direct physical source: relying on its initial [] is
                # unsafe because another qualified reference may already have
                # narrowed that source's projection. Derived sources retain
                # the independently collected dependencies of their leaf
                # scopes.
                if not col.table:
                    if len(scope_bindings) > 1:
                        _force_full_for_scope(scope)
                        _record_schema_dependent_binding(scope, col_name)
                continue

            if (
                resolved_alias
                and col_name not in seen_per_alias[resolved_alias]
                and resolved_alias not in table_star_aliases
            ):
                seen_per_alias[resolved_alias].add(col_name)
                alias_to_columns[resolved_alias].append(col_name)

        for owner, using_names in using_columns_by_owner.items():
            if owner in table_star_aliases:
                continue
            for name in using_names:
                if name not in seen_per_alias[owner]:
                    seen_per_alias[owner].add(name)
                    alias_to_columns[owner].append(name)

        # Apply star semantics after collection: star always wins.
        for alias in table_star_aliases | force_full_projection:
            alias_to_columns[alias] = []

        # An unqualified WHERE/GROUP BY/QUALIFY/window-spec name that also names
        # a SELECT alias is schema-dependent: DuckDB gives a same-named
        # physical input column precedence while accepting the alias when the
        # input is absent.
        # Make the conservative decision at the shared parser boundary so
        # every estimator/engine receives the physical schema it may need;
        # fixing only one backend's loader leaves AUTO-routed engines with the
        # same missing-column failure (and understates their scan estimate).
        # Sort columns for aliases that are not "all columns".
        for alias, cols in alias_to_columns.items():
            if cols:  # leave [] as special "all columns"
                alias_to_columns[alias] = sorted(cols)

        self._alias_to_columns = alias_to_columns

    # ---------------- Public API ----------------

    def get_table_tuples(self) -> List[TableDefinition]:
        """
        Return a list of TableDefinition instances:

            TableDefinition(
                super_name: str,
                simple_name: str,
                alias: str,
                columns: List[str]   # [] means "all columns" when derived from * / t.*
            )
        """
        result: List[TableDefinition] = []

        for alias, (supertable, table_name) in self._alias_to_table.items():
            columns = self._alias_to_columns.get(alias, [])
            definition = TableDefinition(
                super_name=supertable,
                simple_name=table_name,
                alias=alias,
                columns=columns,
            )
            result.append(definition)

        return result

    def get_group_alias_ambiguities(self) -> Dict[str, Set[str]]:
        """Return schema-dependent alias/input-name binding collisions.

        The compatibility name predates WHERE/QUALIFY/window-spec support.
        Callers must treat the returned mapping as read-only. A defensive copy
        keeps parser state immutable when engines prepare a query.
        """
        return self.get_binding_ambiguities()

    def get_binding_ambiguities(self) -> Dict[str, Set[str]]:
        """Return schema-dependent unqualified-name binding candidates.

        Each name must remain visible on every listed physical binding.  A
        protected view that removes one can change backend name resolution,
        turning an originally invalid/physical reference into a data-bearing
        alias or whole-row expression.  Executors use this map to deny that
        semantic change before creating protected relations.
        """
        return {
            alias: set(columns)
            for alias, columns in self._binding_ambiguities.items()
        }

    def get_physical_tables(self) -> List[TableDefinition]:
        """
        Return deduplicated *physical* tables with merged columns.

        This method is designed for RBAC and engine reflection creation
        where the question is "which real tables does this query touch,
        and which columns from each?"

        Differences from ``get_table_tuples()``:

        1. **CTE aliases are excluded.**  A reference to ``summary`` in
           ``WITH summary AS (SELECT … FROM orders) SELECT … FROM summary``
           is not a physical table.  The real table (``orders``) is
           returned instead — with columns collected from inside the CTE body.

        2. **Same table, multiple aliases → merged.**
           ``FROM orders a JOIN orders b ON …`` produces a single entry
           for ``orders`` whose column list is the union of both aliases.

        3. **Star semantics propagate.**  If *any* alias for a table has
           ``[]`` (meaning ``SELECT *`` or ``t.*``), the merged result
           is ``[]`` (all columns).

        4. **The ``alias`` field is set to ``simple_name``** since the
           per-alias distinction is meaningless after merging.

        Downstream callers:
        - ``restrict_read_access()``  — RBAC column/table validation.
        - ``DataEstimator``           — snapshot resolution (one per table).
        - Engine reflection creation  — one ``parquet_scan`` per table.

        For alias-level operations (query rewriting, view naming) continue
        using ``get_table_tuples()``.
        """
        # Group by (super_name, simple_name), merge columns across aliases.
        merged: Dict[Tuple[str, str], List[str]] = {}

        for alias, (super_name, table_name) in self._alias_to_table.items():
            # Skip aliases that the scope graph proves are only CTE
            # references. A same-named physical source inside a CTE remains.
            if alias not in self._physical_aliases:
                continue

            key = (super_name, table_name)
            cols = self._alias_to_columns.get(alias, [])

            if key not in merged:
                merged[key] = list(cols)
            else:
                existing = merged[key]
                if not existing or not cols:
                    # [] means star (all columns) — star wins.
                    merged[key] = []
                else:
                    combined = set(existing) | set(cols)
                    merged[key] = sorted(combined)

        result: List[TableDefinition] = []
        for (super_name, table_name), columns in merged.items():
            result.append(TableDefinition(
                super_name=super_name,
                simple_name=table_name,
                alias=table_name,
                columns=columns,
            ))

        return result

    # ---------------- Predicate extraction (read-path pruning) ----------------

    def _conjunct_to_constraint(
        self,
        node: exp.Expression,
        alias_to_phys: Dict[str, Tuple[str, str]],
        single_alias: Optional[str],
    ) -> Optional[Tuple[str, str, PredInterval]]:
        """Turn one WHERE conjunct into ``(alias, column, PredInterval)``.

        Only simple ``column OP literal`` comparisons, ``BETWEEN`` and ``IN``
        (literal list) are recognised — and only when the column resolves to a
        direct table source in the current scope.  Everything else (functions on
        the column, ``!=``, ``IS NULL``, sub-selects, OR-branches) returns
        ``None`` so the file is conservatively retained.
        """
        node = _unwrap_paren(node)

        def _resolve(col: exp.Column) -> Optional[str]:
            qualifier = col.table
            if qualifier:
                return qualifier if qualifier in alias_to_phys else None
            return single_alias

        # column OP literal  (or literal OP column)
        op = _COMPARISON_OPS.get(type(node))
        if op is not None:
            left, right = node.left, node.right
            if isinstance(left, exp.Column) and not isinstance(right, exp.Column):
                col, lit, flip = left, right, False
            elif isinstance(right, exp.Column) and not isinstance(left, exp.Column):
                col, lit, flip = right, left, True
            else:
                return None
            alias = _resolve(col)
            if alias is None:
                return None
            lane_value = _literal_to_lane_value(lit)
            if lane_value is None:
                return None
            lane, value = lane_value
            if self.dialect == "spark" and lane in {
                "numeric_cast", "date", "timestamp", "timestamptz",
            }:
                return None
            if flip:
                op = _FLIP_OP[op]
            return alias, col.name, _interval_for_op(op, lane, value)

        # column BETWEEN low AND high
        if isinstance(node, exp.Between):
            col = node.this
            if not isinstance(col, exp.Column):
                return None
            alias = _resolve(col)
            if alias is None:
                return None
            lo_lv = _literal_to_lane_value(node.args.get("low"))
            hi_lv = _literal_to_lane_value(node.args.get("high"))
            if lo_lv is None or hi_lv is None or lo_lv[0] != hi_lv[0]:
                return None
            if self.dialect == "spark" and lo_lv[0] in {
                "numeric_cast", "date", "timestamp", "timestamptz",
            }:
                return None
            return alias, col.name, PredInterval(lo_lv[0], lo_lv[1], True, hi_lv[1], True)

        # column IN (literal, literal, ...)
        if isinstance(node, exp.In):
            col = node.this
            if not isinstance(col, exp.Column) or node.args.get("query"):
                return None
            alias = _resolve(col)
            if alias is None:
                return None
            exprs = node.args.get("expressions") or []
            if not exprs:
                return None
            lanes_values = [_literal_to_lane_value(e) for e in exprs]
            if any(lv is None for lv in lanes_values):
                return None
            lanes = {lv[0] for lv in lanes_values}
            if len(lanes) != 1:
                return None
            lane = lanes.pop()
            if self.dialect == "spark" and lane in {
                "numeric_cast", "date", "timestamp", "timestamptz",
            }:
                return None
            values = [lv[1] for lv in lanes_values]
            return alias, col.name, PredInterval(lane, min(values), True, max(values), True)

        return None

    def get_predicate_constraints(self) -> Dict[Tuple[str, str], List[Dict[str, PredInterval]]]:
        """Per physical table, the list of per-scan constraint dicts.

        Keyed by ``(super_name.lower(), simple_name.lower())``.  Each list entry
        is one *occurrence* of that table (an alias in some query scope) mapping
        ``column → PredInterval`` for the conjuncts that constrain it; an empty
        dict means that occurrence is unconstrained (so the table must be read in
        full).  Read-path pruning unions across occurrences: a file is dropped
        only when **every** occurrence excludes it.

        Always returns safely — any parse/scope error yields ``{}`` (no
        pruning), so this never breaks a query.
        """
        result: Dict[Tuple[str, str], List[Dict[str, PredInterval]]] = {}
        try:
            scopes = self._get_pruning_scopes()
        except Exception:
            return result

        for scope in scopes:
            select = scope.expression
            if not isinstance(select, exp.Select):
                # sqlglot gives an aliased parenthesized joined-table group,
                # e.g. ``(b JOIN c ON ...) x``, its own non-SELECT scope.  The
                # executor still registers one shared physical view for b.  If
                # b is also filtered through another alias outside the group,
                # omitting this unconstrained occurrence would prune files x
                # needs.  We cannot safely map predicates through the group's
                # derived alias, so record each direct physical source as an
                # empty (full-scan) occurrence.
                for src in scope.sources.values():
                    if not isinstance(src, exp.Table):
                        continue
                    db = self._get_db_name(src) or self.default_super_name
                    key = (db.lower(), src.name.lower())
                    result.setdefault(key, []).append({})
                continue

            alias_to_phys: Dict[str, Tuple[str, str]] = {}
            for name, src in scope.sources.items():
                if isinstance(src, exp.Table):
                    db = self._get_db_name(src) or self.default_super_name
                    alias_to_phys[name] = (db, src.name)
            if not alias_to_phys:
                continue

            single_alias = (
                next(iter(alias_to_phys))
                if len(alias_to_phys) == 1 and len(scope.sources) == 1
                else None
            )

            per_alias: Dict[str, Dict[str, PredInterval]] = {a: {} for a in alias_to_phys}
            where = select.args.get("where")
            if where is not None:
                for conj in _split_and(where.this):
                    parsed = self._conjunct_to_constraint(conj, alias_to_phys, single_alias)
                    if parsed is None:
                        continue
                    alias, col, interval = parsed
                    d = per_alias[alias]
                    if col in d:
                        merged = _intersect_intervals(d[col], interval)
                        if merged is None:
                            del d[col]
                        else:
                            d[col] = merged
                    else:
                        d[col] = interval

            for alias, (db, tbl) in alias_to_phys.items():
                key = (db.lower(), tbl.lower())
                result.setdefault(key, []).append(per_alias[alias])

        return result

    # ---------------- Join-edge extraction (cross-table file pruning) ----------------

    @staticmethod
    def _join_prunability(join: exp.Join) -> Tuple[bool, bool]:
        """``(own_prunable, outer_prunable)`` for one JOIN node.

        ``own`` is the join node's own operand (the table written after the JOIN
        keyword — the null-supplying side of a LEFT join); ``outer`` is
        everything already joined before it.  An endpoint may be pruned only
        when its rows are NOT preserved without a match:

        * INNER / comma / CROSS+ON — both sides must match → both prunable.
        * SEMI — an existence filter; result rows on either side must match →
          both prunable (regardless of a LEFT/RIGHT spelling).
        * ANTI — the preserved side's NON-matching rows are the result → it is
          never prunable; the probe side only feeds the existence test, so
          dropping its provably-unmatchable files is sound.
        * LEFT / RIGHT / FULL OUTER — the preserved side(s) keep every row
          (null-extended), so only a null-supplying side is prunable.
        """
        side = (join.args.get("side") or "").upper()
        kind = (join.args.get("kind") or "").upper()
        if kind == "SEMI":
            return True, True
        if kind == "ANTI":
            # Preserved side = outer, unless spelled RIGHT ANTI.
            return (False, True) if side == "RIGHT" else (True, False)
        if side == "LEFT":
            return True, False
        if side == "RIGHT":
            return False, True
        if side == "FULL":
            return False, False
        return True, True

    def get_join_edges(self) -> List[JoinEdge]:
        """Equi-join links (``a.x = b.y``) between two *different* physical tables.

        Collected from every scope's ``JOIN ... ON`` / ``USING`` conditions and
        its ``WHERE`` clause (the ``FROM a, b WHERE a.x = b.y`` form).  Only
        ``=`` between two **alias-qualified** columns that resolve to distinct
        tables in the same scope is an edge; anything else (``a.x < b.y`` range
        joins, ``col = literal``, self-joins, an unqualified side, functions,
        db-qualified 3-part references — DuckDB parses the struct-field access
        ``a.s.id`` the same way, so they cannot be bound safely) is ignored.
        ``USING (c)`` is translated only when the accumulated left side is a
        single plain table (the first hop), where the equality is unambiguous.
        Alias resolution is case-insensitive (DuckDB identifiers are).

        Soundness gating — each endpoint carries a *prunable* flag:

        * the preserved side of an outer/anti join is never prunable (its
          non-matching rows appear in the result), per
          :meth:`_join_prunability`; a WHERE-clause equality is null-rejecting
          (effectively inner), so it grants both sides.  Grants for the same
          endpoint pair OR together — they all constrain the same scope's
          result;
        * a physical table bound MORE than once (second alias, UNION branch,
          subquery — any scope) is never prunable: the executor scans one
          shared file list for all occurrences, and pruning it to what one
          occurrence needs would drop files another occurrence reads.

        Each :class:`JoinEdge` keys its endpoints by
        ``(super_name.lower(), simple_name.lower())`` — the same key
        :meth:`get_predicate_constraints` emits — so the result maps directly
        onto the per-table file/stats dicts a cross-table pruner works with.

        Always returns safely: any parse/scope error yields ``[]`` (no join
        pruning), so this never breaks a query.
        """
        try:
            scopes = self._get_pruning_scopes()
        except Exception:
            return []

        # Occurrence census + per-scope alias maps (lowercased alias -> key).
        # A lowercase alias collision (illegal in DuckDB, but be defensive)
        # poisons that alias to None so nothing resolves through it.
        occurrence_count: Dict[Tuple[str, str], int] = {}
        scope_aliases: List[Dict[str, Optional[Tuple[str, str]]]] = []
        for scope in scopes:
            alias_to_phys: Dict[str, Optional[Tuple[str, str]]] = {}
            for name, src in scope.sources.items():
                if not isinstance(src, exp.Table):
                    continue
                db = self._get_db_name(src) or self.default_super_name
                key = (db.lower(), src.name.lower())
                # Count physical sources in every scope, including sqlglot's
                # non-SELECT scope for an aliased parenthesized table group
                # such as ``(b JOIN c) x``.  The executor still gives every
                # occurrence of b one shared physical file list; missing the
                # grouped occurrence would let an edge on another alias prune
                # files that x needs.
                occurrence_count[key] = occurrence_count.get(key, 0) + 1
                if isinstance(scope.expression, exp.Select):
                    lname = name.lower()
                    alias_to_phys[lname] = (
                        None if lname in alias_to_phys else key
                    )
            scope_aliases.append(alias_to_phys)

        # edge dedup key -> [endpoints, l_prunable, r_prunable]; grants OR.
        edges: Dict[Tuple, List] = {}

        def add_edge(
            l_key, l_col, r_key, r_col, l_prunable: bool, r_prunable: bool
        ) -> None:
            endpoints = sorted([(l_key, l_col, l_prunable), (r_key, r_col, r_prunable)],
                               key=lambda e: (e[0], e[1]))
            dedup_key = ((endpoints[0][0], endpoints[0][1]),
                         (endpoints[1][0], endpoints[1][1]))
            entry = edges.get(dedup_key)
            if entry is None:
                edges[dedup_key] = [endpoints[0][2], endpoints[1][2]]
            else:
                entry[0] = entry[0] or endpoints[0][2]
                entry[1] = entry[1] or endpoints[1][2]

        def add_eq_conjunct(
            node: exp.Expression,
            alias_to_phys: Dict[str, Optional[Tuple[str, str]]],
            own_aliases: Optional[Set[str]],
            own_prunable: bool,
            outer_prunable: bool,
        ) -> None:
            node = _unwrap_paren(node)
            if not isinstance(node, exp.EQ):
                return
            left, right = node.left, node.right
            if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
                return
            # A db-qualified column (a.s.id) is ambiguous: sqlglot binds it by
            # its MIDDLE segment, but DuckDB may resolve it as a struct-field
            # access on table a.  Never build an edge from one.
            if left.args.get("db") or right.args.get("db"):
                return
            la, ra = left.table.lower(), right.table.lower()
            if not la or not ra:
                return  # both sides must be qualified to resolve a table
            l_key = alias_to_phys.get(la)
            r_key = alias_to_phys.get(ra)
            if l_key is None or r_key is None:
                return
            if l_key == r_key:
                return  # self-join: no cross-table propagation
            # Directional joins are safe only when the join node's complete
            # RHS ownership is known.  In particular, treating every endpoint
            # as the accumulated/outer side is unsound for RIGHT / RIGHT ANTI:
            # their RHS is preserved.  Opaque derived tables and other complex
            # operands therefore produce no asymmetric edge rather than a
            # guessed direction.
            if own_aliases is None and own_prunable != outer_prunable:
                return
            add_edge(
                l_key, left.name.lower(), r_key, right.name.lower(),
                own_prunable if own_aliases is not None and la in own_aliases
                else outer_prunable,
                own_prunable if own_aliases is not None and ra in own_aliases
                else outer_prunable,
            )

        def join_operand_aliases(
            node: exp.Expression,
            alias_to_phys: Dict[str, Optional[Tuple[str, str]]],
        ) -> Optional[Set[str]]:
            """Physical aliases structurally owned by one JOIN operand.

            sqlglot represents an unaliased parenthesized join such as
            ``(b JOIN c ON ...)`` as ``Subquery(Table(b, joins=[c]))`` in the
            *same* scope.  Both ``b`` and ``c`` belong to that JOIN node's own
            side, which matters for RIGHT/FULL/ANTI preservation semantics.

            Only that transparent table/join-group shape is traversed.  A real
            SELECT/UNION subquery, an aliased group (which sqlglot scopes
            separately), or any alias that cannot be resolved to a physical
            table returns ``None`` so asymmetric pruning fails closed.
            """
            node = _unwrap_paren(node)
            if isinstance(node, exp.Subquery):
                if node.alias:
                    return None
                return join_operand_aliases(node.this, alias_to_phys)
            if not isinstance(node, exp.Table):
                return None

            alias = (node.alias or node.name).lower()
            if alias_to_phys.get(alias) is None:
                return None
            aliases = {alias}
            for nested_join in node.args.get("joins") or []:
                nested = join_operand_aliases(nested_join.this, alias_to_phys)
                if nested is None:
                    return None
                aliases.update(nested)
            return aliases

        for scope, alias_to_phys in zip(scopes, scope_aliases):
            select = scope.expression
            if not isinstance(select, exp.Select):
                continue
            if sum(1 for v in alias_to_phys.values() if v is not None) < 2:
                continue  # need at least two real tables for a cross-table join

            # The accumulated left side, for USING resolution: starts with the
            # FROM operand, grows by each join's own operand.
            from_expr = select.args.get("from")
            preceding: List[str] = []
            if from_expr is not None and isinstance(from_expr.this, exp.Table):
                preceding.append((from_expr.this.alias or from_expr.this.name).lower())

            for join in select.args.get("joins") or []:
                own_aliases = join_operand_aliases(join.this, alias_to_phys)
                own_alias = (
                    next(iter(own_aliases))
                    if own_aliases is not None and len(own_aliases) == 1
                    else None
                )
                own_prunable, outer_prunable = self._join_prunability(join)

                on = join.args.get("on")
                if on is not None:
                    for conj in _split_and(on):
                        add_eq_conjunct(
                            conj, alias_to_phys, own_aliases,
                            own_prunable, outer_prunable,
                        )

                # USING (c): own.c = other.c — only safe when the accumulated
                # left side is a single plain table (both sides then provably
                # carry column c).  Deeper chain hops would need schema
                # knowledge to bind c to the right left-side table.
                using = join.args.get("using") or []
                if using and own_alias is not None and len(preceding) == 1:
                    l_key = alias_to_phys.get(preceding[0])
                    r_key = alias_to_phys.get(own_alias)
                    if l_key is not None and r_key is not None and l_key != r_key:
                        for ident in using:
                            col = ident.name.lower()
                            add_edge(
                                l_key, col, r_key, col,
                                outer_prunable, own_prunable,
                            )

                preceding.append(own_alias if own_alias is not None else "")

            # WHERE equality is null-rejecting → effectively inner: both sides
            # prunable (even after an outer join — rows failing it are gone).
            where = select.args.get("where")
            if where is not None:
                for conj in _split_and(where.this):
                    add_eq_conjunct(conj, alias_to_phys, set(), True, True)

        result: List[JoinEdge] = []
        for ((l_key, l_col), (r_key, r_col)), (l_pr, r_pr) in edges.items():
            result.append(JoinEdge(
                l_key, l_col, r_key, r_col,
                prune_left=l_pr and occurrence_count.get(l_key, 0) == 1,
                prune_right=r_pr and occurrence_count.get(r_key, 0) == 1,
            ))
        return result
