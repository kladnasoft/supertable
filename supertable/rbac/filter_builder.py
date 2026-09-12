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


def _render_operand(spec: dict, operation: str) -> str:
    """Render the right-hand side of a single RBAC filter comparison.

    There are exactly three operand types and each has exactly one rendering:

    * ``null``      → the SQL ``NULL`` keyword
    * ``value``     → a single-quoted, escaped string literal
    * ``reference`` → a **quoted identifier** (another column)

    Any other ``type`` is rejected.  The old ``else`` branch interpolated the
    operand *unquoted* for every non-``value`` type, which was a row-filter
    bypass (S6): ``{"amount": {"operation": ">", "type": "reference",
    "value": "0 OR 1=1"}}`` rendered as ``"amount" > 0 OR 1=1``, a tautology
    that removed the restriction instead of applying it.  ``_sanitize_value``
    did not catch it because the payload contains no quote, semicolon or
    comment marker — it is ordinary SQL, and the defect was that it was
    allowed to *be* SQL at all.
    """
    val_type = spec.get("type")

    if val_type == "null":
        return "NULL"

    if val_type == "value":
        escape_clause = ""
        if operation in ("ILIKE", "NOT ILIKE") and "escape" in spec:
            escape_clause = f" ESCAPE '{_sanitize_value(spec['escape'])}'"
        return f"'{_sanitize_value(spec['value'])}'{escape_clause}"

    if val_type == "reference":
        # A reference names another column, so it is validated and quoted the
        # same way the left-hand side is.
        return _sanitize_column(str(spec["value"]))

    raise ValueError(f"Invalid operand type in RBAC filter: {val_type!r}")


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
                        range_parts.append(
                            f"{safe_col} {safe_op} {_render_operand(cond, safe_op)}"
                        )
                    clauses.append(" AND ".join(range_parts))
                else:
                    safe_col = _sanitize_column(key)
                    operation = _sanitize_operation(val["operation"])
                    value = _render_operand(val, operation)
                    clauses.append(f"{safe_col} {operation} {value}")
            return " AND ".join(clauses)
        else:
            return ""

    def build_filter_query(self, table_name, columns, filters):
        """Render the role's row filter into a ``SELECT … WHERE`` statement.

        ``["*"]`` — and an absent ``filters`` key, which defaults to it — is
        the one and only way to say "no row restriction".  Any *other* filter
        value is a configured policy, and a configured policy that renders to
        an empty predicate is a broken policy, not an unrestricted one: it
        raises rather than silently dropping the ``WHERE`` (S9).

        ``[{}]``, ``[{"AND": []}]``, ``[[]]``, ``[]`` and ``{}`` all rendered
        to ``""`` before, and an empty predicate string meant no view was
        built at all — a role-level row-security policy the operator believed
        was in force applied nothing, with no exception and no log line.
        """
        column_list = format_column_list(columns)

        if filters == ["*"]:
            where_clause = ""
        else:
            predicates = self.json_to_sql_clause(filters)
            if not predicates:
                raise ValueError(
                    f"RBAC row filter rendered to an empty predicate: {filters!r}. "
                    f"A configured filter must produce SQL; use ['*'] to mean "
                    f"unrestricted."
                )
            where_clause = f"\nWHERE {predicates}"

        return f"SELECT {column_list}\nFROM {table_name}{where_clause}"