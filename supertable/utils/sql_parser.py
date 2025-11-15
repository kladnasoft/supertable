from typing import Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError
from supertable.data_classes import TableDefinition


class SQLParser:
    """
    Minimal SQL parser for extracting table/column mappings.

    Input:
        SQLParser(super_name: str, query: str)

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

    def __init__(self, super_name: str, query: str):
        if not super_name or not isinstance(super_name, str):
            raise ValueError("Parameter 'super_name' must be a non-empty string.")

        if not query or not isinstance(query, str):
            raise ValueError("Parameter 'query' must be a non-empty SQL string.")

        self.default_super_name: str = super_name
        self.original_query: str = query

        # Internal parsed expression
        self._parsed: exp.Expression = self._parse_query(query)

        # alias -> (supertable, table)
        self._alias_to_table: Dict[str, Tuple[str, str]] = {}

        # alias -> ordered unique list of column names
        # (or [] if meaning "all columns" due to * or t.*)
        self._alias_to_columns: Dict[str, List[str]] = {}

        self._extract_tables()
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
                first_line = str(error).strip().splitlines()[0]
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
    def _parse_query(query: str) -> exp.Expression:
        try:
            return sqlglot.parse_one(query)
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

    # ---------------- Column extraction helpers ----------------

    @staticmethod
    def _is_direct_alias_projection_column(col: exp.Column) -> bool:
        """
        True if this Column is the direct value of an Alias in SELECT
        (e.g. "o.id AS order_id"), so we don't double-count it.
        """
        parent = col.parent
        return isinstance(parent, exp.Alias) and parent.this is col

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
        for col in self._parsed.find_all(exp.Column):
            col_name = col.name
            if not col_name or col_name == "*":
                # Skip stars; they are handled via star logic.
                continue

            if self._is_direct_alias_projection_column(col):
                # Already counted from SELECT list.
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
