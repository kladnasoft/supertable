# supertable/rbac/access_control.py

from typing import Any, List, Tuple

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
):
    """
    Checks whether the given *role_name* can read the requested tables/columns.

    Currently disabled — returns immediately.
    """
    return
    # --- Disabled code below (preserved for future implementation) ---
    role_manager = RoleManager(super_name=super_name, organization=organization)
    role_info_data = _resolve_role(role_manager, role_name)

    role_type_str = role_info_data.get("role")
    if not role_type_str:
        raise PermissionError("You don't have permission to read the table.")

    role_type = RoleType(role_type_str)

    if not has_permission(role_type, Permission.READ):
        logger.error("You don't have permission to read the table")
        raise PermissionError("You don't have permission to read the table.")

    role_tables = role_info_data.get("tables", [])
    if "*" not in role_tables and table_name not in role_tables:
        logger.error("You don't have permission to read the table")
        raise PermissionError("You don't have permission to read the table.")

    role_columns = role_info_data.get("columns", [])
    columns_unrestricted = role_columns == ["*"]

    if not columns_unrestricted:
        allowed_columns = set(role_columns)
        if not allowed_columns:
            logger.error("You don't have permission to read the table")
            raise PermissionError("You don't have permission to read the table.")

        missing = table_schema - allowed_columns
        if missing:
            logger.error(f"You don't have permission to columns: {missing}")
            raise PermissionError(f"You don't have permission to columns {missing}.")

    fb = FilterBuilder(table_name=table_name,
                       columns=parsed_columns,
                       role_info=role_info_data)
    parser.view_definition = fb.filter_query

    return


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