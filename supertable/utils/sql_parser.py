import secrets
import sqlglot
from sqlglot import exp, to_identifier
from sqlglot.errors import ParseError
from sqlglot.optimizer.scope import build_scope
from supertable.utils.timer import Timer


class SQLParser:
    def __init__(self, query):
        self.original_query = query
        self.parsed_query = None
        self.executing_query = None
        self.reflection_table = f"reflection_{secrets.token_hex(8)}"
        self.rbac_view = f"rbac_{secrets.token_hex(8)}"
        self.view_definition = None
        self.original_table = None
        self.columns_select = None
        self.columns_where = None
        self.columns_order = None
        self.columns_list = None
        self.columns_csv = None

    timer = Timer()

    def _select_columns(self, select_: exp.Select, skip: set[str]) -> list[str]:
        """
        Return the physical columns produced by `SELECT …`, preserving order.
        If the projection contains a star ( *  or  tbl.* ), return ['*'].
        """
        # 1️⃣  star present?  → nothing else matters
        if any(item.is_star for item in select_.expressions):
            return []

        # 2️⃣  otherwise fall back to your previous logic
        seen, cols = set(), []
        for col in select_.find_all(exp.Column):
            name = col.name
            if name == "*" or name in skip or name in seen:
                continue
            seen.add(name)
            cols.append(name)
        return cols

    def _collect_cols(self, node: exp.Expression | None,
                      skip: set[str]) -> list[str]:
        """
        Walk `node` and return every physical column name that
        • isn’t '*'
        • isn’t one of the SELECT‑list aliases (“skip”)
        The order is preserved and duplicates are removed.
        """
        if node is None:
            return []

        seen, cols = set(), []
        for c in node.find_all(exp.Column):
            name = c.name
            if name == "*" or name in skip or name in seen:
                continue
            seen.add(name)
            cols.append(name)
        return cols

    def physical_tables(self, parsed) -> list[str]:
        """
        Return each distinct physical table referenced anywhere in `sql`,
        ignoring CTEs and sub‑select aliases. Order is alphabetical.
        """
        root = build_scope(parsed)
        return sorted({
            source.name  # e.g. 'used_cars'
            for scope in root.traverse()  # walk every scope (main + CTEs)
            for _, source in scope.selected_sources.values()
            if isinstance(source, exp.Table)  # ← only real tables have this type
        })


    #@timer
    def parse_sql(self):
        try:
            # Attempt to parse the SQL query
            parsed = sqlglot.parse_one(self.original_query)
            self.parsed_query = str(parsed)

            # Every alias that the SELECT list introduces (AS ... or bare alias)
            aliased = {a.alias for a in parsed.find_all(exp.Alias) if a.alias}
            select_node = parsed.find(exp.Select)
            self.columns_select = self._select_columns(select_node, aliased)
            self.columns_where = self._collect_cols(parsed.find(exp.Where), aliased)
            self.columns_order = self._collect_cols(parsed.find(exp.Order), aliased)

            # Extract tables
            tables = self.physical_tables(parsed)

            # Check for multiple tables in the query
            if len(tables) > 1:
                raise ValueError("Only 1 table can be in the query")

            self.original_table = tables[0] if tables else None
            if not self.original_table:
                raise ValueError("No table found in the query")

            # Replace table names with a fixed name
            for table in parsed.find_all(sqlglot.expressions.Table):
                original_name = table.name  # e.g. "pick_true_majority_tree"

                if original_name in tables:
                    # if the query didn’t give the table an alias, add one now
                    if not table.alias:
                        table.set(
                            "alias",
                            exp.TableAlias(this=to_identifier(original_name))
                        )

                    # finally replace the table name with the RBAC‑filtered view
                    table.set("this", to_identifier(self.rbac_view))


            if len(self.columns_select) == 0:
                self.columns_list = ["*"]
            else:
                self.columns_list = list(set(self.columns_select + self.columns_where + self.columns_order))

            self.columns_csv = ",".join(self.columns_list)
            self.executing_query = str(parsed)

        except ParseError as e:
            raise ValueError(f"Failed to parse SQL query: {e}")
        except Exception as e:
            raise ValueError(f"An error occurred while parsing SQL query: {e}")
