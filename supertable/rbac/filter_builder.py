import re

_SAFE_VALUE_RE = re.compile(r"^[A-Za-z0-9_.@\-+:/ ,%()]+$")
_SAFE_COLUMN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ALLOWED_OPS = frozenset({
    "=", "!=", "<>", "<", ">", "<=", ">=",
    "LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE",
    "IN", "NOT IN", "IS", "IS NOT",
    "BETWEEN", "NOT BETWEEN",
})
_COMPARISON_OPS = frozenset({"=", "!=", "<>", "<", ">", "<=", ">="})
_LIKE_OPS = frozenset({"LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE"})
_LIST_OPS = frozenset({"IN", "NOT IN"})
_BETWEEN_OPS = frozenset({"BETWEEN", "NOT BETWEEN"})


def _sanitize_column(col: str, quote_char: str = '"') -> str:
    """Validate and quote a column name used in RBAC filters."""
    if quote_char not in ('"', '`'):
        raise ValueError("Unsupported RBAC identifier quote")
    if not isinstance(col, str):
        raise ValueError("Invalid column name in RBAC filter")
    col = col.strip()
    if not col or not _SAFE_COLUMN_RE.fullmatch(col):
        raise ValueError("Invalid column name in RBAC filter")
    return f"{quote_char}{col}{quote_char}"


def _sanitize_table_identifier(table_name: str, quote_char: str = '"') -> str:
    """Validate and quote a bare SimpleTable name used in an RBAC query."""
    if quote_char not in ('"', '`'):
        raise ValueError("Unsupported RBAC identifier quote")
    if not isinstance(table_name, str):
        raise ValueError("Invalid table name in RBAC filter")
    table_name = table_name.strip()
    if (
        not 1 <= len(table_name) <= 128
        or not _SAFE_COLUMN_RE.fullmatch(table_name)
    ):
        raise ValueError("Invalid table name in RBAC filter")
    return f"{quote_char}{table_name}{quote_char}"


def _sanitize_value(val: str) -> str:
    """Escape a string value for safe SQL embedding in RBAC filters."""
    # Replace single quotes with doubled single quotes (SQL standard escaping)
    escaped = str(val).replace("'", "''")
    # Block semicolons, comment markers, and other injection vectors
    if any(c in escaped for c in (";", "--", "/*", "*/")):
        raise ValueError("Disallowed characters in RBAC filter value")
    return escaped


def _sanitize_operation(op: str) -> str:
    """Validate a SQL operation used in RBAC filters."""
    if not isinstance(op, str):
        raise ValueError("RBAC filter operation must be a string")
    normalized = op.strip().upper()
    if normalized not in _ALLOWED_OPS:
        raise ValueError("Invalid operation in RBAC filter")
    return normalized


def format_column_list(columns, quote_char='"'):
    if quote_char not in ('"', '`'):
        raise ValueError("Unsupported RBAC identifier quote")
    if not isinstance(columns, list):
        raise ValueError("RBAC columns must be a list")
    if columns == ["*"]:
        return "*"
    if not columns or "*" in columns:
        raise ValueError("RBAC wildcard must be the only selected column")
    quoted = [_sanitize_column(column, quote_char) for column in columns]
    return ",".join(f'{column} as {column}' for column in quoted)


class FilterBuilder():
    def __init__(
            self, table_name: str, columns: list, role_info: dict,
            identifier_quote: str = '"',
    ):
        if identifier_quote not in ('"', '`'):
            raise ValueError("Unsupported RBAC identifier quote")
        self.identifier_quote = identifier_quote
        filters = role_info.get("filters", ["*"])
        self.filter_query = self.build_filter_query(table_name, columns, filters)

    def _column(self, value: str) -> str:
        return _sanitize_column(value, self.identifier_quote)

    def json_to_sql_clause(self, json_obj, _depth=0):
        if _depth > 32:
            raise ValueError("RBAC filter nesting is too deep")
        if isinstance(json_obj, list):
            if not json_obj:
                raise ValueError("RBAC filter list cannot be empty")
            parts = []
            for item in json_obj:
                part = self.json_to_sql_clause(item, _depth + 1)
                parts.append(part)
            return " AND ".join(parts)
        elif isinstance(json_obj, dict):
            if not json_obj:
                raise ValueError("RBAC filter object cannot be empty")
            clauses = []
            for key, val in json_obj.items():
                if key in ("AND", "OR"):
                    if not isinstance(val, list) or not val:
                        raise ValueError(f"RBAC {key} filter requires a non-empty list")
                    nested = [
                        f"({self.json_to_sql_clause(item, _depth + 1)})"
                        for item in val
                    ]
                    clauses.append(f" {key} ".join(nested))
                elif key == "NOT":
                    clauses.append(
                        f"NOT ({self.json_to_sql_clause(val, _depth + 1)})"
                    )
                elif not isinstance(val, dict):
                    raise ValueError("Invalid RBAC predicate for column")
                elif "range" in val:
                    if set(val) != {"range"}:
                        raise ValueError("RBAC range cannot contain extra fields")
                    if not isinstance(val["range"], list) or not val["range"]:
                        raise ValueError("RBAC range requires at least one bound")
                    safe_col = self._column(key)
                    range_parts = []
                    for cond in val["range"]:
                        if not isinstance(cond, dict):
                            raise ValueError("Invalid RBAC range bound")
                        if set(cond) != {"operation", "type", "value"}:
                            raise ValueError(
                                "RBAC range bounds require exactly operation, "
                                "type, and value"
                            )
                        safe_op = _sanitize_operation(cond["operation"])
                        if safe_op not in _COMPARISON_OPS:
                            raise ValueError(
                                "RBAC range bounds require a comparison operation"
                            )
                        val_type = cond.get("type")
                        if val_type == "value":
                            if isinstance(cond["value"], (list, dict)):
                                raise ValueError("RBAC range values must be scalar")
                            safe_val = _sanitize_value(cond["value"])
                            range_parts.append(f"{safe_col} {safe_op} '{safe_val}'")
                        elif val_type == "reference":
                            safe_ref = self._column(cond["value"])
                            range_parts.append(f"{safe_col} {safe_op} {safe_ref}")
                        else:
                            raise ValueError(
                                f"Invalid RBAC range value type: {val_type!r}"
                            )
                    clauses.append(" AND ".join(range_parts))
                else:
                    safe_col = self._column(key)
                    if not {"operation", "type"}.issubset(val):
                        raise ValueError("Incomplete RBAC predicate")
                    operation = _sanitize_operation(val["operation"])
                    val_type = val["type"]
                    if val_type == "null":
                        # Older persisted policies sometimes carried an unused
                        # empty ``value`` alongside type=null.  Accept only
                        # that harmless spelling; any substantive value or
                        # other field remains an error.
                        allowed_null_fields = {"operation", "type"}
                        if "value" in val and val["value"] in (None, ""):
                            allowed_null_fields.add("value")
                        if set(val) != allowed_null_fields:
                            raise ValueError(
                                "NULL RBAC predicates accept only operation and type"
                            )
                        if operation not in ("IS", "IS NOT"):
                            raise ValueError("NULL RBAC filters require IS or IS NOT")
                        value = "NULL"
                    elif val_type == "value":
                        if "value" not in val:
                            raise ValueError("RBAC predicate has no value")
                        allowed_fields = {"operation", "type", "value"}
                        if operation in _LIKE_OPS:
                            allowed_fields.add("escape")
                        if set(val) - allowed_fields:
                            raise ValueError(
                                f"RBAC predicate for {key!r} has unsupported fields"
                            )
                        if operation in ("IS", "IS NOT"):
                            raise ValueError(
                                "IS/IS NOT RBAC predicates require type 'null'"
                            )
                        raw_value = val["value"]
                        if operation in _LIST_OPS:
                            if not isinstance(raw_value, list) or not raw_value:
                                raise ValueError(
                                    f"RBAC {operation} requires a non-empty value list"
                                )
                            items = ", ".join(
                                f"'{_sanitize_value(item)}'" for item in raw_value
                            )
                            value = f"({items})"
                        elif operation in _BETWEEN_OPS:
                            if not isinstance(raw_value, list) or len(raw_value) != 2:
                                raise ValueError(
                                    f"RBAC {operation} requires exactly two values"
                                )
                            lo, hi = (_sanitize_value(item) for item in raw_value)
                            value = f"'{lo}' AND '{hi}'"
                        else:
                            if isinstance(raw_value, (list, dict)):
                                raise ValueError(
                                    f"RBAC {operation} requires one scalar value"
                                )
                            escape_clause = ""
                            if operation in _LIKE_OPS and "escape" in val:
                                escape_raw = val["escape"]
                                if not isinstance(escape_raw, str) or len(escape_raw) != 1:
                                    raise ValueError("RBAC LIKE escape must be one character")
                                esc_char = _sanitize_value(escape_raw)
                                escape_clause = f" ESCAPE '{esc_char}'"
                            safe_val = _sanitize_value(raw_value)
                            value = f"'{safe_val}'{escape_clause}"
                    elif val_type == "reference":
                        if set(val) != {"operation", "type", "value"}:
                            raise ValueError(
                                "Reference RBAC predicates require exactly operation, "
                                "type, and value"
                            )
                        if "value" not in val:
                            raise ValueError("RBAC predicate has no reference")
                        if operation not in (_COMPARISON_OPS | _LIKE_OPS):
                            raise ValueError(
                                f"RBAC {operation} does not accept a column reference"
                            )
                        value = self._column(val["value"])
                    else:
                        raise ValueError("Invalid RBAC value type")
                    clauses.append(f"{safe_col} {operation} {value}")
            return " AND ".join(clauses)
        else:
            raise ValueError("RBAC filter must be an object or list")

    def build_filter_query(self, table_name, columns, filters):
        safe_table = _sanitize_table_identifier(
            table_name, self.identifier_quote,
        )
        column_list = format_column_list(columns, self.identifier_quote)

        if filters == ["*"]:
            where_clause = ""
        else:
            predicates = self.json_to_sql_clause(filters)
            if not predicates:
                raise ValueError("RBAC filter produced no predicate")
            where_clause = f"\nWHERE {predicates}"

        return f"SELECT {column_list}\nFROM {safe_table}{where_clause}"
