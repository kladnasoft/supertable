def format_column_list(columns):
    if columns == ["*"]:  # Check if the columns list is just a wildcard
        return "*"
    else:
        # Join the columns with the desired format
        return ",".join(f'"{column}" as "{column}"' for column in columns)


class FilterBuilder():
    def __init__(self, table_name: str, columns: list, role_info: dict):
        filters = role_info.get("filters", ["*"])
        self.filter_query = self.build_filter_query(table_name, columns, filters)

    def json_to_sql_clause(self, json_obj):
        if isinstance(json_obj, list):
            return "".join([self.json_to_sql_clause(item) for item in json_obj])
        elif isinstance(json_obj, dict):
            clauses = []
            for key, val in json_obj.items():
                if key in ["AND", "OR"]:
                    nested_clauses = f" {key} ".join(
                        [f"({self.json_to_sql_clause(nested_item)})" for nested_item in val]
                    )
                    clauses.append(nested_clauses)
                elif key == "NOT":
                    nested_clause = f"NOT ({self.json_to_sql_clause(val)})"
                    clauses.append(nested_clause)
                elif "range" in val:
                    range_clauses = " AND ".join(
                        [
                            (
                                f"{key} {cond['operation']} '{cond['value']}'"
                                if cond["type"] == "value"
                                else f"{key} {cond['operation']} {cond['value']}"
                            )
                            for cond in val["range"]
                        ]
                    )
                    clauses.append(range_clauses)
                else:
                    operation = val["operation"]
                    escape_clause = ""
                    if operation.upper() == "ILIKE" and "escape" in val:
                        escape_clause = f" ESCAPE '{val['escape'] * 2}'"
                    value = (
                        f"'{val['value']}'{escape_clause}"
                        if val["type"] == "value"
                        else val["value"]
                    )
                    if val["type"] == "null":
                        value = "NULL"
                    clause = f"{key} {operation} {value}"
                    clauses.append(clause)
            return "".join(clauses)
        else:
            return ""

    def build_filter_query(self, table_name, columns, filters):
        column_list = format_column_list(columns)
        predicates = self.json_to_sql_clause(filters)
        where_clause = f"\nWHERE {predicates}" if predicates else ""

        query = f"""SELECT {column_list}
FROM {table_name}{where_clause}"""

        return query

