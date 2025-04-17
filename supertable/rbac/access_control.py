from typing import Any

from supertable.config.defaults import logger
from supertable.rbac.user_manager import UserManager
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.permissions import has_permission, Permission, RoleType
from supertable.rbac.filter_builder import FilterBuilder
from supertable.utils.sql_parser import SQLParser

def check_write_access(super_name: str, organization: str, user_hash: str, table_name: str) -> None:
    """
    Checks whether the user (identified by user_hash) is allowed to perform
    a WRITE operation on the specified table_name.

    If the user does not have the necessary permission, raises a PermissionError.

    Conditions:
      1) The user must have at least one role granting WRITE permission.
      2) That WRITE-capable role must list this table_name in its 'tables',
         or contain '*' to allow access to all tables.

    :param organization:
    :param super_name:  Base directory name (used to instantiate managers).
    :param user_hash:   The unique user ID/Hash in your system.
    :param table_name:  The name of the table the user attempts to write to.
    :return: None if permitted; otherwise raises PermissionError.
    """

    user_manager = UserManager(super_name=super_name, organization=organization)
    role_manager = RoleManager(super_name=super_name, organization=organization)

    # 1. Load the user data (which includes the role hashes)
    try:
        user_data = user_manager.get_user(user_hash)
    except ValueError:
        # The user_hash might be invalid or not found
        logger.error(f"User not found: {user_hash}")
        raise PermissionError("Invalid or nonexistent user.")

    role_hashes = user_data.get("roles", [])
    if not role_hashes:
        # The user has no roles assigned => no write access
        logger.error(f"You don't have permission to write to this table.: {role_hashes}")
        raise PermissionError("You don't have permission to write to this table.")

    # 2. For each role, check WRITE permission and table access
    for r_hash in role_hashes:
        role_info = role_manager.get_role(r_hash)
        if not role_info:
            # This role_hash might be invalid or missing
            continue

        role_type_str = role_info.get("role")
        role_type = RoleType(role_type_str)

        # Check if this role grants WRITE
        if has_permission(role_type, Permission.WRITE):
            # Now check table access
            allowed_tables = role_info.get("tables", [])
            if "*" in allowed_tables or table_name in allowed_tables:
                # Found a role that grants WRITE & includes this table
                return  # No error => user is allowed to write

    # If no matching role was found, deny
    logger.error(f"You don't have permission to write to this table.")
    raise PermissionError("You don't have permission to write to this table.")


def restrict_read_access(
    super_name: str,
    organization: str,
    user_hash: str,
    table_name: str,
    table_schema: set,
    parsed_columns: list[str],
    parser: SQLParser,
):
    """
    Checks whether the user (identified by user_hash) can read 'table_name'.

    If the user is missing permission for the table or if some columns in 'schema'
    aren't permitted by at least one role, this method raises PermissionError.

    Otherwise, it returns None, indicating the user is allowed full access to 'schema'.
    """

    role_info = []
    user_manager = UserManager(super_name=super_name, organization=organization)
    role_manager = RoleManager(super_name=super_name, organization=organization)

    # Try getting the user
    try:
        user_data = user_manager.get_user(user_hash)
    except ValueError:
        # User not found or invalid user_hash
        logger.error(f"User not found: {user_hash}")
        raise PermissionError("User does not exist or is invalid.")

    role_hashes = user_data.get("roles", [])
    if not role_hashes:
        # No roles => user can't read anything
        logger.error(f"You don't have permission to read the table: {user_hash}")
        raise PermissionError("You don't have permission to read the table.")

    # We'll gather column sets across all roles and unify them.
    allowed_columns = set()
    columns_unrestricted = False  # True if any role has ["*"]

    # Track if there's at least one role that grants read access.
    has_read_access = False

    for r_hash in role_hashes:
        role_info = role_manager.get_role(r_hash)
        if not role_info:
            # Possibly an invalid role hash
            continue

        role_type_str = role_info.get("role")
        if not role_type_str:
            continue

        role_type = RoleType(role_type_str)

        # 1) Does this role grant READ?
        if not has_permission(role_type, Permission.READ):
            continue

        # 2) Does this role cover the requested table?
        tables = role_info.get("tables", [])
        if "*" not in tables and table_name not in tables:
            continue

        # If we got here, the role DOES grant read access to this table.
        has_read_access = True

        # Merge columns from this role
        role_columns = role_info.get("columns", [])
        if role_columns == ["*"]:
            # Means "all columns"
            columns_unrestricted = True
        else:
            allowed_columns.update(role_columns)

    # If no role grants read access at all, deny:
    if not has_read_access:
        logger.error(f"You don't have permission to read the table")
        raise PermissionError("You don't have permission to read the table.")

    # If columns_unrestricted is True, user has access to all columns.
    # But if false, we must check for missing columns.
    if not columns_unrestricted:
        # If allowed_columns is empty, user effectively has no columns → no access
        if not allowed_columns:
            logger.error(f"You don't have permission to read the table")
            raise PermissionError("You don't have permission to read the table.")

        # If some columns in 'schema' aren't in allowed_columns, raise error
        missing = table_schema - allowed_columns
        if missing:
            logger.error(f"You don't have permission to columns: {missing}")
            raise PermissionError(f"You don't have permission to columns {missing}.")


    fb = FilterBuilder(table_name =table_name,
                       columns=parsed_columns,
                       role_info=role_info)
    parser.view_definition = fb.filter_query

    # If we arrive here without raising an error, user is fully permitted to read all columns in 'schema'.
    return


def check_meta_access(super_name: str, organization: str, user_hash: str, table_name: str) -> None:
    """
    Checks whether the user (identified by user_hash) is allowed to perform
    a META operation (i.e., an ALTER-style change) on the specified table_name.

    If the user does not have the necessary permission, raises a PermissionError.

    Conditions:
      1) The user must have at least one role granting ALTER permission.
      2) That ALTER-capable role must list this table_name in its 'tables',
         or contain '*' to allow access to all tables.

    :param organization:
    :param super_name:  Base directory name (used to instantiate managers).
    :param user_hash:   The unique user ID/Hash in your system.
    :param table_name:  The name of the table the user attempts to alter.
    :return: None if permitted; otherwise raises PermissionError.
    """

    user_manager = UserManager(super_name=super_name, organization=organization)
    role_manager = RoleManager(super_name=super_name, organization=organization)

    # 1. Load the user data (which includes the role hashes)
    try:
        user_data = user_manager.get_user(user_hash)
    except ValueError:
        # The user_hash might be invalid or not found
        logger.error(f"User not found: {user_hash}")
        raise PermissionError("Invalid or nonexistent user.")

    role_hashes = user_data.get("roles", [])
    if not role_hashes:
        # The user has no roles => no meta access
        logger.error(f"You don't have permission to alter this table: {role_hashes}")
        raise PermissionError("You don't have permission to alter this table.")

    # 2. For each role, check ALTER permission and table access
    for r_hash in role_hashes:
        role_info = role_manager.get_role(r_hash)
        if not role_info:
            # This role_hash might be invalid or missing
            continue

        role_type_str = role_info["role"]
        role_type = RoleType(role_type_str)

        # Check if this role grants the META permission
        if has_permission(role_type, Permission.META):
            # Now check table coverage
            allowed_tables = role_info.get("tables", [])
            if "*" in allowed_tables or table_name in allowed_tables:
                # Found a role that grants META & includes this table
                return  # No error => user can perform META ops

    # If no matching role was found, deny
    logger.error(f"You don't have permission to META data.")
    raise PermissionError("You don't have permission to META data.")
