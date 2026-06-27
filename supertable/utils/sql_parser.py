# route: supertable.utils.sql_parser
from datetime import date, datetime
from typing import Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError
from sqlglot.optimizer.scope import traverse_scope
from supertable.data_classes import PredInterval, TableDefinition


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


def _parse_datetime_literal(s: str) -> Optional[datetime]:
    """Parse an ISO-ish date/datetime string into a tz-naive microsecond
    ``datetime``; ``None`` when it doesn't look like a date."""
    txt = s.strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(txt, fmt)
        except ValueError:
            continue
    return None


def _literal_to_lane_value(node: exp.Expression) -> Optional[Tuple[str, object]]:
    """Reduce a literal-ish expression to ``(lane, value)`` or ``None``.

    Lanes: ``numeric`` (ints/floats/bools), ``string`` (quoted strings) and
    ``timestamp`` (DATE/TIMESTAMP casts of date-shaped strings).  Any expression
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
            type_name = str(to.this).upper()
        inner = node.this
        if any(t in type_name for t in ("DATE", "TIMESTAMP", "DATETIME")):
            if isinstance(inner, exp.Literal) and inner.is_string:
                dt = _parse_datetime_literal(inner.this)
                return ("timestamp", dt) if dt is not None else None
            return None
        # Numeric / textual cast → fall through to the inner literal.
        return _literal_to_lane_value(inner)

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
            - Resolved via the table alias from FROM/JOIN.
        - Unqualified columns:
            - If there is exactly one table in the query, they are attributed
              to that table.
            - If multiple tables exist, unqualified columns are ignored
              as ambiguous.
        - For SELECT projections with aliases, e.g. "o.id AS order_id":
            - We record "id" for alias "o".
        - Star handling:
            - SELECT *       -> all aliases: []
            - SELECT t.*     -> alias t: []
            - Never record "*" as a physical column name.
        - We do not record columns for non-Column expressions.
    """

    SUPPORTED_DIALECTS = ("duckdb", "spark")

    def __init__(self, super_name: str, query: str, dialect: str):
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

        # Internal parsed expression
        self._parsed: exp.Expression = self._parse_query(query, dialect)

        # alias -> (supertable, table)
        self._alias_to_table: Dict[str, Tuple[str, str]] = {}

        # alias -> ordered unique list of column names
        # (or [] if meaning "all columns" due to * or t.*)
        self._alias_to_columns: Dict[str, List[str]] = {}

        self._extract_tables()
        self._cte_names: Set[str] = self._collect_cte_names()
        self._extract_columns()

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
            return sqlglot.parse_one(query, read=dialect)
        except ParseError as e:
            message = SQLParser._build_parse_error_message(e)
            raise ValueError(f"Failed to parse SQL query: {message}") from None
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

    # ---------------- Table extraction ----------------

    def _extract_tables(self) -> None:
        """
        Build alias -> (supertable, table) mapping.

        Rules:
            - If table has explicit schema (e.g. stock.orders), use that.
            - Otherwise, prefix with default supertable.
            - If no alias is present, alias = table name.
        """
        alias_to_table: Dict[str, Tuple[str, str]] = {}

        for table in self._parsed.find_all(exp.Table):
            table_name = table.name
            if not table_name:
                continue

            db_name = self._get_db_name(table) or self.default_super_name
            alias = self._get_alias(table) or table_name

            alias_to_table[alias] = (db_name, table_name)

        if not alias_to_table:
            raise ValueError("No tables found in SQL query.")

        self._alias_to_table = alias_to_table

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
    def _is_direct_alias_projection_column(col: exp.Column) -> bool:
        """
        True if this Column is the direct value of an Alias in SELECT
        (e.g. "o.id AS order_id"), so we don't double-count it.
        """
        parent = col.parent
        return isinstance(parent, exp.Alias) and parent.this is col

    @staticmethod
    def _is_inside_alias_scope(col: exp.Column) -> bool:
        """
        True if this Column lives inside a clause where SELECT alias
        references are legal in standard SQL: ORDER BY, HAVING, or QUALIFY.

        Walking up the AST from the Column node, if we hit one of these
        clause types before reaching the Select node, the column is in
        alias scope and may be a reference to a computed SELECT alias
        rather than a physical table column.
        """
        node = col.parent
        while node is not None:
            if isinstance(node, (exp.Order, exp.Having, exp.Qualify)):
                return True
            if isinstance(node, exp.Select):
                # Reached the SELECT without passing through ORDER/HAVING/QUALIFY
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

        # Determine if we can safely assign unqualified columns
        unique_tables = set(self._alias_to_table.values())
        single_alias_for_unqualified: Optional[str] = None
        if len(unique_tables) == 1:
            # All aliases refer to the same physical table -> unqualified columns OK.
            single_alias_for_unqualified = next(iter(self._alias_to_table.keys()))

        select_expr = self._parsed.find(exp.Select)

        # ---------------- Detect * and t.* in SELECT ----------------

        global_star = False
        table_star_aliases: Set[str] = set()

        if select_expr is not None:
            for proj in select_expr.expressions:
                # Case 1: explicit Star node
                if isinstance(proj, exp.Star):
                    # SELECT *  -> proj.this is None
                    # SELECT t.* -> proj.this holds the qualifier
                    if proj.this is None:
                        global_star = True
                        break
                    else:
                        # t.* case via Star(this=...)
                        table_ref = proj.this
                        table_alias: Optional[str] = None

                        if isinstance(table_ref, exp.Identifier):
                            table_alias = table_ref.name
                        elif hasattr(table_ref, "name"):
                            table_alias = table_ref.name

                        if table_alias and table_alias in self._alias_to_table:
                            table_star_aliases.add(table_alias)

                # Case 2: some sqlglot versions may represent t.* as Column(name="*", table="t")
                elif isinstance(proj, exp.Column) and proj.name == "*":
                    table_alias = proj.table
                    if table_alias:
                        if table_alias in self._alias_to_table:
                            table_star_aliases.add(table_alias)
                    else:
                        # Bare "*" as Column fallback -> treat as global star
                        global_star = True
                        break

        # Global * overrides everything: all tables => all columns ([])
        if global_star:
            self._alias_to_columns = {alias: [] for alias in self._alias_to_table}
            return

        # ---------------- Normal column extraction (no global *) ----------------

        if select_expr is not None:
            for proj in select_expr.expressions:
                # We already interpreted all star forms above; skip them here
                if isinstance(proj, exp.Star):
                    continue
                if isinstance(proj, exp.Column) and proj.name == "*":
                    # Star-like Column already handled in detection; skip.
                    continue

                if isinstance(proj, exp.Alias):
                    # Aliased projection: e.g. "o.id AS order_id"
                    value_expr = proj.this
                    if isinstance(value_expr, exp.Column):
                        col = value_expr
                        col_name = col.name
                        if not col_name or col_name == "*":
                            # Ignore bogus or star-like columns here.
                            continue

                        table_alias = col.table
                        resolved_alias: Optional[str] = None

                        if table_alias and table_alias in alias_to_columns:
                            resolved_alias = table_alias
                        elif not table_alias and single_alias_for_unqualified:
                            resolved_alias = single_alias_for_unqualified

                        if (
                            resolved_alias
                            and col_name not in seen_per_alias[resolved_alias]
                            and resolved_alias not in table_star_aliases
                        ):
                            seen_per_alias[resolved_alias].add(col_name)
                            alias_to_columns[resolved_alias].append(col_name)
                    # Non-Column expressions in aliases are ignored.
                else:
                    # Non-aliased projections: capture Column children
                    for col in proj.find_all(exp.Column):
                        col_name = col.name
                        if not col_name or col_name == "*":
                            # Do not treat "*" as a real column.
                            continue

                        table_alias = col.table
                        resolved_alias: Optional[str] = None

                        if table_alias and table_alias in alias_to_columns:
                            resolved_alias = table_alias
                        elif not table_alias and single_alias_for_unqualified:
                            resolved_alias = single_alias_for_unqualified

                        if (
                            resolved_alias
                            and col_name not in seen_per_alias[resolved_alias]
                            and resolved_alias not in table_star_aliases
                        ):
                            seen_per_alias[resolved_alias].add(col_name)
                            alias_to_columns[resolved_alias].append(col_name)

        # 2) Handle remaining Column nodes (WHERE, JOIN, GROUP BY, ORDER BY, etc.)
        #
        # Collect SELECT-list alias names so we can recognise references to
        # computed columns in ORDER BY / HAVING / QUALIFY.  These aliases are
        # NOT physical table columns and must not be added to the column set.
        select_alias_names: Set[str] = set()
        if select_expr is not None:
            for proj in select_expr.expressions:
                if isinstance(proj, exp.Alias):
                    alias_ident = proj.args.get("alias")
                    if isinstance(alias_ident, exp.Identifier) and alias_ident.name:
                        select_alias_names.add(alias_ident.name.lower())

        for col in self._parsed.find_all(exp.Column):
            col_name = col.name
            if not col_name or col_name == "*":
                # Skip stars; they are handled via star logic.
                continue

            if self._is_direct_alias_projection_column(col):
                # Already counted from SELECT list.
                continue

            # Skip references to SELECT aliases in clauses where alias
            # references are legal (ORDER BY, HAVING, QUALIFY).  These are
            # not physical table columns.
            if (
                col_name.lower() in select_alias_names
                and not col.table
                and self._is_inside_alias_scope(col)
            ):
                continue

            table_alias = col.table
            resolved_alias: Optional[str] = None

            if table_alias and table_alias in alias_to_columns:
                resolved_alias = table_alias
            elif not table_alias and single_alias_for_unqualified:
                resolved_alias = single_alias_for_unqualified
            else:
                # Ambiguous unqualified column with multiple tables -> ignore.
                continue

            if (
                resolved_alias
                and col_name not in seen_per_alias[resolved_alias]
                and resolved_alias not in table_star_aliases
            ):
                seen_per_alias[resolved_alias].add(col_name)
                alias_to_columns[resolved_alias].append(col_name)

        # 3) Apply t.* semantics: any alias with t.* means "all columns"
        for alias in table_star_aliases:
            alias_to_columns[alias] = []

        # 4) Sort columns for aliases that are not "all columns"
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
            # Skip CTE aliases — they are not physical tables.
            if table_name in self._cte_names:
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
            scopes = traverse_scope(self._parsed)
        except Exception:
            return result

        for scope in scopes:
            select = scope.expression
            if not isinstance(select, exp.Select):
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
