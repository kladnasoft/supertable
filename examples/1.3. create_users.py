import os
from supertable.config.defaults import logger
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.user_manager import UserManager

from examples.defaults import super_name, organization

# ---------- ROLE OPERATIONS ----------
# Initialize the RoleManager with a base directory.
role_manager = RoleManager(super_name=super_name, organization=organization)

# List valid roles from _roles.json.
valid_roles = role_manager.list_roles()
for role in valid_roles:
    logger.info(f"Role Type: {role['role']}, Hash: {role['hash']}")


# Extract valid role hashes by role type.
admin_role_hash = None
editor_role_hash = None
viewer_role_hash = None
usage_role_hash = None

for role in valid_roles:
    role_type = role["role"].lower()
    if role_type == "admin" and admin_role_hash is None:
        admin_role_hash = role["hash"]
    elif role_type == "writer" and editor_role_hash is None:
        editor_role_hash = role["hash"]
    elif role_type == "reader" and viewer_role_hash is None:
        viewer_role_hash = role["hash"]
    elif role_type == "meta" and usage_role_hash is None:
        usage_role_hash = role["hash"]

# If no viewer role exists, create one.
if viewer_role_hash is None:
    viewer_data = {
        "role": "reader",
        "tables": [],      # Empty list interpreted as all tables
        "columns": [],     # Empty list interpreted as all columns
        "filters": {}      # No filters = all rows
    }
    viewer_role_hash = role_manager.create_role(viewer_data)
    logger.info(f"Default Viewer role created with hash: {viewer_role_hash}")

# ---------- USER OPERATIONS ----------
# Initialize the UserManager with the same base directory.
user_manager = UserManager(super_name=super_name, organization=organization)

# --- Create users ---
# User Alice will have the admin and editor roles.
alice_data = {
    "username": "alice",
    "roles": [admin_role_hash, editor_role_hash]  # valid role hashes
}
alice_hash = user_manager.create_user(alice_data)
logger.info(f"User Alice created with hash: {alice_hash}")

# User Bob will have the viewer role.
bob_data = {
    "username": "bob",
    "roles": [viewer_role_hash]
}
bob_hash = user_manager.create_user(bob_data)
logger.info(f"User Bob created with hash: {bob_hash}")

# User Charlie is created with no roles.
charlie_data = {
    "username": "charlie",
    "roles": []
}
charlie_hash = user_manager.create_user(charlie_data)
logger.info(f"User Charlie created with hash: {charlie_hash}")

# --- Modify user ---
# Update Charlie: change his username and assign him the usage role.
user_manager.modify_user(charlie_hash, {"username": "charlie_updated", "roles": [usage_role_hash]})
charlie_data_updated = user_manager.get_user(charlie_hash)
logger.info(f"User Charlie after modification: {charlie_data_updated}")

# --- Delete a user ---
# Delete Bob.
user_manager.delete_user(bob_hash)
logger.info(f"User Bob deleted: {bob_hash}")


# --- Delete a role ---
# For example, delete the viewer role.
if viewer_role_hash:
    deleted = role_manager.delete_role(viewer_role_hash)
    if deleted:
        logger.info(f"Viewer role deleted: {viewer_role_hash}")
        # Remove the deleted role from all users.
        user_manager.remove_role_from_users(viewer_role_hash)
        logger.info(f"Viewer role removed from all users: {viewer_role_hash}")
    else:
        logger.error(f"Viewer role deletion failed: {viewer_role_hash}")

# --- List all users ---
logger.info(f"Listing all users:")
user_meta = user_manager.storage.read_json(user_manager.user_meta_path)
for user_hash, username in user_meta["users"].items():
    user_file_path = os.path.join(user_manager.user_dir, user_hash + ".json")
    user_data = user_manager.storage.read_json(user_file_path)
    print()
    logger.info(user_data)
