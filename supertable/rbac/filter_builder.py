import re

_SAFE_VALUE_RE = re.compile(r"^[A-Za-z0-9_.@\-+:/ ,%()]+$")
_SAFE_COLUMN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ALLOWED_OPS = frozenset({
    "=", "!=", "<>", "<", ">", "<=", ">=",
    "LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE",
    "IN", "NOT IN", "IS", "IS NOT",
    "BETWEEN", "NOT BETWEEN",
})


def _sanitize_column(col: str) -> str:
    """Validate and quote a column name used in RBAC filters."""
    col = col.strip()
    if not col or not _SAFE_COLUMN_RE.fullmatch(col):
        raise ValueError(f"Invalid column name in RBAC filter: {col!r}")
    return f'"{col}"'


def _sanitize_value(val: str) -> str:
    """Escape a string value for safe SQL embedding in RBAC filters."""
    # Replace single quotes with doubled single quotes (SQL standard escaping)
    escaped = str(val).replace("'", "''")
    # Block semicolons, comment markers, and other injection vectors
    if any(c in escaped for c in (";", "--", "/*", "*/")):
        raise ValueError(f"Disallowed characters in RBAC filter value: {val!r}")
    return escaped


def _sanitize_operation(op: str) -> str:
    """Validate a SQL operation used in RBAC filters."""
    normalized = op.strip().upper()
    if normalized not in _ALLOWED_OPS:
        raise ValueError(f"Invalid operation in RBAC filter: {op!r}")
    return normalized


def format_column_list(columns):
    if columns == ["*"]:
        return "*"
    else:
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
                    safe_col = _sanitize_column(key)
                    range_parts = []
                    for cond in val["range"]:
                        safe_op = _sanitize_operation(cond["operation"])
                        if cond["type"] == "value":
                            safe_val = _sanitize_value(cond["value"])
                            range_parts.append(f"{safe_col} {safe_op} '{safe_val}'")
                        else:
                            safe_val = _sanitize_value(str(cond["value"]))
                            range_parts.append(f"{safe_col} {safe_op} {safe_val}")
                    clauses.append(" AND ".join(range_parts))
                else:
                    safe_col = _sanitize_column(key)
                    operation = _sanitize_operation(val["operation"])
                    val_type = val["type"]
                    if val_type == "null":
                        value = "NULL"
                    elif val_type == "value":
                        escape_clause = ""
                        if operation in ("ILIKE", "NOT ILIKE") and "escape" in val:
                            esc_char = _sanitize_value(val["escape"])
                            escape_clause = f" ESCAPE '{esc_char}'"
                        safe_val = _sanitize_value(val["value"])
                        value = f"'{safe_val}'{escape_clause}"
                    else:
                        value = _sanitize_value(str(val["value"]))
                    clauses.append(f"{safe_col} {operation} {value}")
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