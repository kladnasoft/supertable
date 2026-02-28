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
admin_id = role_manager.create_role(admin_data)
logger.info(f"Admin role created with id: {admin_id}")

editor_data = {
    "role": "writer",
    "tables": ["table1", "table2"]
}
editor_id = role_manager.create_role(editor_data)
logger.info(f"Editor role created with id: {editor_id}")

usage_data = {
    "role": "meta",
    "tables": ["table1"]
}
usage_id = role_manager.create_role(usage_data)
logger.info(f"Usage role created with id: {usage_id}")

viewer_data = {
    "role": "reader",
    "tables": ["table1"],
    "columns": ["name", "email", "age"],
    "filters": {"country": "US", "active": True}
}
viewer_id = role_manager.create_role(viewer_data)
logger.info(f"Viewer role created with id: {viewer_id}")

viewer2_data = {
    "role": "reader",
    "tables": ["table2", "table3"],
    "columns": ["name", "email", "age"],
    "filters": {"country": "EU", "active": False}
}
viewer2_id = role_manager.create_role(viewer2_data)
logger.info(f"Viewer2 role created with id: {viewer2_id}")

# --- Update a role in-place (no new id) ---
logger.info(f"Updating viewer role {viewer_id} to add 'phone' column...")
role_manager.update_role(viewer_id, {
    "columns": ["name", "email", "age", "phone"],
})
updated_viewer = role_manager.get_role(viewer_id)
logger.info(f"Viewer role after update: {updated_viewer}")

# --- List all roles ---
all_roles = role_manager.list_roles()
logger.info(f"Listing all roles ({len(all_roles)}):")
for role in all_roles:
    logger.info(role)

# --- Retrieve a specific role's configuration ---
admin_config = role_manager.get_role(admin_id)
logger.info(f"Admin config: {admin_config}")