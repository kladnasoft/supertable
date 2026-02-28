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
            parts = []
            for item in json_obj:
                part = self.json_to_sql_clause(item)
                if part:
                    parts.append(part)
            return " AND ".join(parts)
        elif isinstance(json_obj, dict):
            clauses = []
            for key, val in json_obj.items():
                if key in ("AND", "OR"):
                    nested = [f"({self.json_to_sql_clause(item)})" for item in val]
                    clauses.append(f" {key} ".join(nested))
                elif key == "NOT":
                    clauses.append(f"NOT ({self.json_to_sql_clause(val)})")
                elif "range" in val:
                    range_parts = []
                    for cond in val["range"]:
                        if cond["type"] == "value":
                            range_parts.append(f"{key} {cond['operation']} '{cond['value']}'")
                        else:
                            range_parts.append(f"{key} {cond['operation']} {cond['value']}")
                    clauses.append(" AND ".join(range_parts))
                else:
                    operation = val["operation"]
                    val_type = val["type"]
                    if val_type == "null":
                        value = "NULL"
                    elif val_type == "value":
                        escape_clause = ""
                        if operation.upper() == "ILIKE" and "escape" in val:
                            escape_clause = f" ESCAPE '{val['escape'] * 2}'"
                        value = f"'{val['value']}'{escape_clause}"
                    else:
                        value = val["value"]
                    clauses.append(f"{key} {operation} {value}")
            return " AND ".join(clauses)
        else:
            return ""

    def build_filter_query(self, table_name, columns, filters):
        column_list = format_column_list(columns)

        if filters == ["*"]:
            where_clause = ""
        else:
            predicates = self.json_to_sql_clause(filters)
            where_clause = f"\nWHERE {predicates}" if predicates else ""

        return f"SELECT {column_list}\nFROM {table_name}{where_clause}"