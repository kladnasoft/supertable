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
    role_manager = RoleManager(super_name=super_name, organization=organization)
    role_info = _resolve_role(role_manager, role_name)

    role_type_str = role_info.get("role")
    if not role_type_str:
        logger.error(f"Role '{role_name}' has no role type")
        raise PermissionError("You don't have permission to write to this table.")

    role_type = RoleType(role_type_str)

    if not has_permission(role_type, Permission.WRITE):
        logger.error(f"Role '{role_name}' does not have WRITE permission.")
        raise PermissionError("You don't have permission to write to this table.")

    allowed_tables = role_info.get("tables", [])
    if "*" not in allowed_tables and table_name not in allowed_tables:
        logger.error(f"Role '{role_name}' does not cover table '{table_name}' for WRITE.")
        raise PermissionError("You don't have permission to write to this table.")


def restrict_read_access(
        super_name: str,
        organization: str,
        role_name: str,
        tables: List[TableDefinition],
) -> Dict[str, "RbacViewDef"]:
    """
    Check whether *role_name* can read the requested tables/columns.

    Returns a dict of ``{alias: RbacViewDef}`` for each table that needs
    RBAC filtering.  An empty dict means the role is unrestricted
    (superadmin/admin or wildcard columns+filters).

    Raises ``PermissionError`` if the role lacks READ access entirely
    or if a requested table is not in the role's allowed table list.
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

    role_tables = role_info.get("tables", [])
    role_columns = role_info.get("columns", [])
    role_filters = role_info.get("filters", ["*"])

    columns_unrestricted = role_columns == ["*"]
    filters_unrestricted = role_filters == ["*"]

    # If everything is wildcard, no views needed
    if columns_unrestricted and filters_unrestricted and "*" in role_tables:
        return {}

    rbac_views: Dict[str, RbacViewDef] = {}

    for td in tables:
        # Table-level access check
        if "*" not in role_tables and td.simple_name not in role_tables:
            raise PermissionError(
                f"You don't have permission to read table '{td.simple_name}'."
            )

        # Column-level filtering
        allowed_columns = ["*"]
        if not columns_unrestricted:
            if not role_columns:
                raise PermissionError(
                    f"You don't have permission to read any columns in '{td.simple_name}'."
                )
            # If query requests specific columns, validate they're all allowed
            if td.columns:
                requested_lower = {c.lower() for c in td.columns}
                allowed_lower = {c.lower() for c in role_columns}
                denied = requested_lower - allowed_lower
                if denied:
                    raise PermissionError(
                        f"You don't have permission to columns: {denied}"
                    )
            allowed_columns = list(role_columns)

        # Row-level filtering
        where_clause = ""
        if not filters_unrestricted:
            fb = FilterBuilder(
                table_name="__PLACEHOLDER__",  # table name is substituted by executor
                columns=["*"],  # column filtering is separate
                role_info=role_info,
            )
            # Extract just the WHERE clause from the generated query
            generated = fb.filter_query
            where_idx = generated.upper().find("WHERE ")
            if where_idx >= 0:
                where_clause = generated[where_idx + 6:]  # everything after "WHERE "

        # Only add an entry if there's actual filtering to apply
        if allowed_columns != ["*"] or where_clause:
            rbac_views[td.alias] = RbacViewDef(
                allowed_columns=allowed_columns,
                where_clause=where_clause,
            )

    return rbac_views


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
    role_manager = RoleManager(super_name=super_name, organization=organization)
    role_info = _resolve_role(role_manager, role_name)

    role_type_str = role_info.get("role")
    if not role_type_str:
        logger.error(f"Role '{role_name}' has no role type")
        raise PermissionError("You don't have permission to META data.")

    role_type = RoleType(role_type_str)

    if not has_permission(role_type, Permission.META):
        logger.error(f"Role '{role_name}' does not have META permission.")
        raise PermissionError("You don't have permission to META data.")

    allowed_tables = role_info.get("tables", [])
    if "*" not in allowed_tables and table_name not in allowed_tables:
        logger.error(f"Role '{role_name}' does not cover table '{table_name}' for META.")
        raise PermissionError("You don't have permission to META data.")