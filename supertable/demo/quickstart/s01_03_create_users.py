"""Create example users and demonstrate user/role management.

At the end we ensure the default superuser exists; subsequent examples
(2.3.2 create_pipe, 4.1.2 task_obsolete_files) resolve a runtime user_hash
through ``UserManager.get_or_create_default_user``.
"""
from supertable.config.defaults import logger
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.user_manager import UserManager

from supertable.demo.quickstart.defaults import super_name, organization


role_manager = RoleManager(super_name=super_name, organization=organization)

# Discover already-created roles so we can attach them to users.
admin_role_id = writer_role_id = reader_role_id = meta_role_id = superadmin_role_id = None
for role in role_manager.list_roles():
    name = role["role"].lower()
    if name == "superadmin" and superadmin_role_id is None:
        superadmin_role_id = role["role_id"]
    elif name == "admin" and admin_role_id is None:
        admin_role_id = role["role_id"]
    elif name == "writer" and writer_role_id is None:
        writer_role_id = role["role_id"]
    elif name == "reader" and reader_role_id is None:
        reader_role_id = role["role_id"]
    elif name == "meta" and meta_role_id is None:
        meta_role_id = role["role_id"]
    logger.info(f"role: {role['role']} id={role['role_id']}")


user_manager = UserManager(super_name=super_name, organization=organization)

# Alice: admin + writer
alice_id = user_manager.create_user({
    "username": "alice",
    "roles": [r for r in (admin_role_id, writer_role_id) if r],
})
logger.info(f"alice created: {alice_id}")

# Bob: reader
bob_id = user_manager.create_user({
    "username": "bob",
    "roles": [reader_role_id] if reader_role_id else [],
})
logger.info(f"bob created: {bob_id}")

# Charlie: starts with no roles, then we add 'meta'
charlie_id = user_manager.create_user({"username": "charlie", "roles": []})
if meta_role_id:
    user_manager.add_role(charlie_id, meta_role_id)
user_manager.modify_user(charlie_id, {"username": "charlie_v2"})
logger.info(f"charlie state: {user_manager.get_user(charlie_id)}")

# Clean up bob to demonstrate deletion
try:
    user_manager.delete_user(bob_id)
    logger.info(f"bob deleted: {bob_id}")
except Exception as e:
    logger.error(f"bob delete failed: {e}")

# Final listing
for user in user_manager.list_users():
    logger.info(
        f"user={user['username']} id={user['user_id']} roles={user.get('roles', [])}"
    )

# Ensure default superuser exists for downstream examples
default_user_id = user_manager.get_or_create_default_user()
logger.info(f"default superuser id: {default_user_id}")
