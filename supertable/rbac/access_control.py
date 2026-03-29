# supertable/rbac/access_control.py

from typing import Any, Dict, List, Tuple

from supertable.config.defaults import logger
from supertable.data_classes import TableDefinition
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.permissions import has_permission, Permission, RoleType
from supertable.rbac.filter_builder import FilterBuilder
from supertable.utils.sql_parser import SQLParser


def _resolve_role(role_manager: RoleManager, role_name: str) -> dict:
    """Resolve a role_name to its role document.

    Raises ``PermissionError`` if the role does not exist.
    """
    role_info = role_manager.get_role_by_name(role_name)
    if not role_info:
        logger.error(f"Role not found: {role_name}")
        raise PermissionError(f"Invalid or nonexistent role: {role_name}")
    return role_info


def _normalize_tables(role_tables) -> dict:
    """Normalise the ``tables`` field from a role document.

    Handles both the **new** per-table dict format and the **legacy**
    list-of-table-names format that may still exist in Redis for roles
    created before the per-table RBAC redesign.

    Legacy ``["*"]``              → ``{"*": {"columns": ["*"], "filters": ["*"]}}``
    Legacy ``["t1", "t2"]``       → ``{"t1": {"columns": ["*"], "filters": ["*"]}, ...}``
    New    ``{"t1": {…}}``        → returned as-is
    Missing / ``None`` / ``{}``   → ``{}``
    """
    if isinstance(role_tables, dict):
        return role_tables
    if isinstance(role_tables, list):
        return {
            t: {"columns": ["*"], "filters": ["*"]}
            for t in role_tables
        }
    return {}


def _resolve_table_entry(role_tables: dict, table_name: str) -> dict:
    """Look up a table's permission entry from the role definition.

    Returns the table-specific entry if it exists, otherwise the ``"*"``
    default entry.  Returns ``None`` if neither exists.
    """
    return role_tables.get(table_name) or role_tables.get("*")


def _check_table_access(role_tables: dict, table_name: str, permission_label: str) -> None:
    """Raise ``PermissionError`` if the table is not covered by the role.

    ``permission_label`` is used in the error message (e.g. "WRITE", "META").
    """
    entry = _resolve_table_entry(role_tables, table_name)
    if entry is None:
        raise PermissionError(
            f"You don't have permission to {permission_label} table '{table_name}'."
        )


def _check_operation_access(
    super_name: str,
    organization: str,
    role_name: str,
    table_name: str,
    permission: Permission,
    label: str,
) -> None:
    """Shared enforcement for table-scoped operations.

    Checks:
      1. Role exists and has a role type.
      2. Role type has the required ``permission`` in the matrix.
      3. ``table_name`` is covered by the role's table definitions.

    Raises ``PermissionError`` on any failure.
    """
    role_manager = RoleManager(super_name=super_name, organization=organization)
    role_info = _resolve_role(role_manager, role_name)

    role_type_str = role_info.get("role")
    if not role_type_str:
        logger.error(f"Role '{role_name}' has no role type")
        raise PermissionError(f"You don't have permission to {label}.")

    role_type = RoleType(role_type_str)

    if not has_permission(role_type, permission):
        logger.error(f"Role '{role_name}' does not have {permission.name} permission.")
        raise PermissionError(f"You don't have permission to {label}.")

    role_tables = _normalize_tables(role_info.get("tables", {}))
    _check_table_access(role_tables, table_name, label)


def check_control_access(
    super_name: str,
    organization: str,
    role_name: str,
    table_name: str,
) -> None:
    """
    Check whether *role_name* is allowed to perform a CONTROL operation
    (e.g. drop table, truncate) on *table_name*.

    Raises ``PermissionError`` if the role lacks the necessary permission.
    """
    _check_operation_access(
        super_name, organization, role_name, table_name,
        Permission.CONTROL, "control this table",
    )


def check_write_access(
    super_name: str,
    organization: str,
    role_name: str,
    table_name: str,
) -> None:
    """
    Check whether *role_name* is allowed to WRITE to *table_name*.

    Raises ``PermissionError`` if the role lacks the necessary permission.
    """
    _check_operation_access(
        super_name, organization, role_name, table_name,
        Permission.WRITE, "write to this table",
    )


def check_meta_access(
    super_name: str,
    organization: str,
    role_name: str,
    table_name: str,
) -> None:
    """
    Check whether *role_name* is allowed to perform a META (ALTER) operation
    on *table_name*.

    Raises ``PermissionError`` if the role lacks the necessary permission.
    """
    _check_operation_access(
        super_name, organization, role_name, table_name,
        Permission.META, "META data",
    )


def restrict_read_access(
        super_name: str,
        organization: str,
        role_name: str,
        tables: List[TableDefinition],
        physical_tables: List[TableDefinition],
) -> Dict[str, "RbacViewDef"]:
    """
    Check whether *role_name* can read the requested tables/columns.

    Validation uses *physical_tables* (CTE-free, merged by
    ``SQLParser.get_physical_tables()``) to check table and column
    access against the role's per-table definitions.

    View definitions are built per *alias* (from *tables* /
    ``SQLParser.get_table_tuples()``) so the engine can create one
    filtered view per alias.

    Returns a dict of ``{alias: RbacViewDef}`` for each alias that needs
    RBAC filtering.  An empty dict means the role is unrestricted
    (superadmin/admin or wildcard columns+filters on all tables).

    Raises ``PermissionError`` if the role lacks READ access entirely,
    if a requested table is not in the role's allowed table set, or if
    a requested column is not in the role's per-table allowed columns.

    Role table definition format::

        role_info["tables"] = {
            "*": {"columns": ["*"], "filters": ["*"]},       # default
            "orders": {"columns": ["id", "amount"], "filters": [...]},
            "customers": {"columns": ["*"], "filters": ["*"]},
        }
    """
    from supertable.data_classes import RbacViewDef

    role_manager = RoleManager(super_name=super_name, organization=organization)
    role_info = _resolve_role(role_manager, role_name)

    role_type_str = role_info.get("role")
    if not role_type_str:
        raise PermissionError("You don't have permission to read the table.")

    role_type = RoleType(role_type_str)

    if not has_permission(role_type, Permission.READ):
        raise PermissionError("You don't have permission to read the table.")

    # Superadmin/admin: no filtering needed
    if role_type in (RoleType.SUPERADMIN, RoleType.ADMIN):
        return {}

    role_tables = _normalize_tables(role_info.get("tables", {}))
    default_entry = role_tables.get("*")

    # Fast path: if the only entry is "*" with all-wildcards, no views needed
    if (
        default_entry
        and len(role_tables) == 1
        and default_entry.get("columns", ["*"]) == ["*"]
        and default_entry.get("filters", ["*"]) == ["*"]
    ):
        return {}

    # ── Phase 1: Validate physical tables (merged, CTE-free) ──────── #
    #
    # This validates the *complete* set of physical columns the query
    # touches per table — columns from SELECT, WHERE, JOIN ON, GROUP BY,
    # HAVING, window functions, ORDER BY — all merged and deduplicated
    # by get_physical_tables().

    for pt in physical_tables:
        table_entry = _resolve_table_entry(role_tables, pt.simple_name)
        if table_entry is None:
            raise PermissionError(
                f"You don't have permission to read table '{pt.simple_name}'."
            )

        allowed_columns = table_entry.get("columns", ["*"])
        if allowed_columns == ["*"]:
            # Unrestricted columns for this table — skip column validation.
            continue

        if not allowed_columns:
            raise PermissionError(
                f"You don't have permission to read any columns in '{pt.simple_name}'."
            )

        # pt.columns is [] when SELECT * or t.* was used.
        # In that case we cannot enumerate the requested columns at parse
        # time — the RBAC view will enforce column projection at execution.
        if pt.columns:
            requested_lower = {c.lower() for c in pt.columns}
            allowed_lower = {c.lower() for c in allowed_columns}
            denied = requested_lower - allowed_lower
            if denied:
                raise PermissionError(
                    f"You don't have permission to columns: {denied} "
                    f"in table '{pt.simple_name}'."
                )

    # ── Phase 2: Build per-alias RbacViewDef ──────────────────────── #
    #
    # Each alias gets its own view definition derived from its physical
    # table's entry in the role.  CTE aliases (which have no physical
    # table entry and no "*" default) are silently skipped — they have
    # already been validated transitively via physical_tables.

    rbac_views: Dict[str, RbacViewDef] = {}

    for td in tables:
        table_entry = _resolve_table_entry(role_tables, td.simple_name)
        if table_entry is None:
            # Not a physical table in the role (CTE alias) — the engine
            # will also skip this alias since it has no snapshot.
            continue

        allowed_columns = table_entry.get("columns", ["*"])
        filters = table_entry.get("filters", ["*"])

        # Row-level filtering from per-table filters
        where_clause = ""
        if filters != ["*"]:
            fb = FilterBuilder(
                table_name="__PLACEHOLDER__",
                columns=["*"],
                role_info={"filters": filters},
            )
            generated = fb.filter_query
            where_idx = generated.upper().find("WHERE ")
            if where_idx >= 0:
                where_clause = generated[where_idx + 6:]

        # Only add an entry if there's actual filtering to apply
        if allowed_columns != ["*"] or where_clause:
            rbac_views[td.alias] = RbacViewDef(
                allowed_columns=list(allowed_columns),
                where_clause=where_clause,
            )

    return rbac_views
