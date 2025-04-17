from supertable.rbac.role_manager import RoleManager
from examples.defaults import super_name, organization
from supertable.config.defaults import logger

# ---------- ROLE OPERATIONS ----------
# Initialize the RoleManager with a base directory.

role_manager = RoleManager(super_name=super_name, organization=organization)

# --- Create roles ---
admin_data = {
    "role": "admin",
    "tables": ["*"]
}
admin_hash = role_manager.create_role(admin_data)
logger.info(f"Admin role created with hash: {admin_hash}")

editor_data = {
    "role": "writer",
    "tables": ["table1", "table2"]
}
editor_hash = role_manager.create_role(editor_data)
logger.info(f"Editor role created with hash: {admin_hash}")

usage_data = {
    "role": "meta",
    "tables": ["table1"]
}
usage_hash = role_manager.create_role(usage_data)
logger.info(f"Usage role created with hash: {usage_hash}")

viewer_data = {
    "role": "reader",
    "tables": ["table1"],
    "columns": ["name", "email", "age"],
    "filters": {"country": "US", "active": True}
}
viewer_hash = role_manager.create_role(viewer_data)
logger.info(f"Viewer role created with hash: {viewer_hash}")

viewer2_data = {
    "role": "reader",
    "tables": ["table2", "table3"],
    "columns": ["name", "email", "age"],
    "filters": {"country": "EU", "active": False}
}
viewer2_hash = role_manager.create_role(viewer2_data)
logger.info(f"Viewer2 role created with hash: {viewer2_hash}")


# --- List all roles ---
all_roles = role_manager.list_roles()
logger.info(f"Listing all roles: {all_roles}")
for role in all_roles:
    logger.info(role)

# --- Retrieve a specific role's configuration ---
admin_config = role_manager.get_role(admin_hash)
logger.info(f"Retrieving Admin role configuration: {admin_config}")
logger.info(f"Admin config: {admin_config}")
