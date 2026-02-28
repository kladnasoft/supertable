from supertable.config.defaults import logger
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.user_manager import UserManager

from examples.defaults import super_name, organization

# ---------- ROLE OPERATIONS ----------
role_manager = RoleManager(super_name=super_name, organization=organization)

# List valid roles from Redis.
valid_roles = role_manager.list_roles()
for role in valid_roles:
    logger.info(f"Role Type: {role['role']}, ID: {role['role_id']}")


# Extract valid role IDs by role type.
admin_role_id = None
editor_role_id = None
viewer_role_id = None
usage_role_id = None
superadmin_role_id = None

for role in valid_roles:
    role_type = role["role"].lower()
    if role_type == "admin" and admin_role_id is None:
        admin_role_id = role["role_id"]
    elif role_type == "writer" and editor_role_id is None:
        editor_role_id = role["role_id"]
    elif role_type == "reader" and viewer_role_id is None:
        viewer_role_id = role["role_id"]
    elif role_type == "meta" and usage_role_id is None:
        usage_role_id = role["role_id"]
    elif role_type == "superadmin" and superadmin_role_id is None:
        superadmin_role_id = role["role_id"]

# If no viewer role exists, create one.
if viewer_role_id is None:
    viewer_data = {
        "role": "reader",
        "tables": [],
        "columns": [],
        "filters": {}
    }
    viewer_role_id = role_manager.create_role(viewer_data)
    logger.info(f"Default Viewer role created with id: {viewer_role_id}")

# ---------- USER OPERATIONS ----------
user_manager = UserManager(super_name=super_name, organization=organization)

# --- Create users ---
# Alice: admin + editor roles.
alice_data = {
    "username": "alice",
    "roles": [admin_role_id, editor_role_id] if admin_role_id and editor_role_id else []
}
alice_id = user_manager.create_user(alice_data)
logger.info(f"User Alice created with id: {alice_id}")

# Bob: viewer role.
bob_data = {
    "username": "bob",
    "roles": [viewer_role_id] if viewer_role_id else []
}
bob_id = user_manager.create_user(bob_data)
logger.info(f"User Bob created with id: {bob_id}")

# Charlie: no roles initially.
charlie_data = {
    "username": "charlie",
    "roles": []
}
charlie_id = user_manager.create_user(charlie_data)
logger.info(f"User Charlie created with id: {charlie_id}")

# --- Modify user ---
# Add the usage role to Charlie atomically.
if usage_role_id:
    user_manager.add_role(charlie_id, usage_role_id)
    charlie_data_updated = user_manager.get_user(charlie_id)
    logger.info(f"User Charlie after adding usage role: {charlie_data_updated}")

# Rename Charlie.
user_manager.modify_user(charlie_id, {"username": "charlie_updated"})
charlie_data_updated = user_manager.get_user(charlie_id)
logger.info(f"User Charlie after rename: {charlie_data_updated}")

# --- Delete a user ---
try:
    user_manager.delete_user(bob_id)
    logger.info(f"User Bob deleted: {bob_id}")
except Exception as e:
    logger.error(f"Error deleting user Bob: {e}")

# --- Delete a role ---
# delete_role now atomically removes the role from all users.
if viewer_role_id:
    try:
        deleted = role_manager.delete_role(viewer_role_id)
        if deleted:
            logger.info(f"Viewer role deleted: {viewer_role_id}")
        else:
            logger.error(f"Viewer role deletion failed: {viewer_role_id}")
    except Exception as e:
        logger.error(f"Error deleting viewer role: {e}")

# --- List all users ---
logger.info("Listing all users:")
try:
    users = user_manager.list_users()
    for user in users:
        logger.info(
            f"User: {user['username']}, ID: {user['user_id']}, "
            f"Roles: {user.get('roles', [])}"
        )
except Exception as e:
    logger.error(f"Error listing users: {e}")

# --- Test getting default superuser ---
try:
    default_user_id = user_manager.get_or_create_default_user()
    if default_user_id:
        default_user_data = user_manager.get_user(default_user_id)
        logger.info(f"Default superuser: {default_user_data}")
    else:
        logger.warning("No default superuser found")
except Exception as e:
    logger.error(f"Error getting default superuser: {e}")