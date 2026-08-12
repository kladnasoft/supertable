# route: supertable.utils.sql_parser
from datetime import date, datetime
from typing import Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError
from sqlglot.optimizer.scope import traverse_scope
from supertable.data_classes import JoinEdge, PredInterval, TableDefinition


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
        inner_lv = _literal_to_lane_value(inner)
        if inner_lv is None:
            return None
        lane, value = inner_lv
        # The engine compares in the CAST's target lane, so the constraint must
        # live there too — falling through to the inner literal's lane would
        # e.g. turn CAST('1.5' AS DOUBLE) into a STRING constraint whose
        # byte-order pruning excludes a numerically matching '1.50'.
        if any(t in type_name for t in
               ("INT", "DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC")):
            if lane == "numeric":
                return inner_lv
            if lane == "string":
                try:
                    text = value.strip()
                    return "numeric", (float(text) if any(
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
            scopes = traverse_scope(self._parsed)
        except Exception:
            return []

        # Occurrence census + per-scope alias maps (lowercased alias -> key).
        # A lowercase alias collision (illegal in DuckDB, but be defensive)
        # poisons that alias to None so nothing resolves through it.
        occurrence_count: Dict[Tuple[str, str], int] = {}
        scope_aliases: List[Dict[str, Optional[Tuple[str, str]]]] = []
        for scope in scopes:
            alias_to_phys: Dict[str, Optional[Tuple[str, str]]] = {}
            if isinstance(scope.expression, exp.Select):
                for name, src in scope.sources.items():
                    if not isinstance(src, exp.Table):
                        continue
                    db = self._get_db_name(src) or self.default_super_name
                    key = (db.lower(), src.name.lower())
                    lname = name.lower()
                    alias_to_phys[lname] = (
                        None if lname in alias_to_phys else key
                    )
                    occurrence_count[key] = occurrence_count.get(key, 0) + 1
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
            own_alias: Optional[str],
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
            add_edge(
                l_key, left.name.lower(), r_key, right.name.lower(),
                own_prunable if la == own_alias else outer_prunable,
                own_prunable if ra == own_alias else outer_prunable,
            )

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
                own_alias: Optional[str] = None
                if isinstance(join.this, exp.Table):
                    own_alias = (join.this.alias or join.this.name).lower()
                own_prunable, outer_prunable = self._join_prunability(join)

                on = join.args.get("on")
                if on is not None:
                    for conj in _split_and(on):
                        add_eq_conjunct(
                            conj, alias_to_phys, own_alias,
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
                    add_eq_conjunct(conj, alias_to_phys, None, True, True)

        result: List[JoinEdge] = []
        for ((l_key, l_col), (r_key, r_col)), (l_pr, r_pr) in edges.items():
            result.append(JoinEdge(
                l_key, l_col, r_key, r_col,
                prune_left=l_pr and occurrence_count.get(l_key, 0) == 1,
                prune_right=r_pr and occurrence_count.get(r_key, 0) == 1,
            ))
        return result
