"""Create the canonical RBAC roles used throughout the example suite.

The 'superadmin' role is *not* created here — it is minted once per
SuperTable by ``RoleManager``'s bootstrap and its type is reserved, so
``create_role`` refuses it. This script looks it up instead; it is the role
referenced via ``examples.defaults.role_name``.

Reader/writer/meta roles are scoped to the ``facts`` SimpleTable so the
read-side examples have something to query.

Administering roles requires an actor holding ``Permission.RBAC``, which is
why the manager is constructed with ``actor_role_name="superadmin"``.
"""
from supertable.rbac.role_manager import RoleManager
from supertable.config.defaults import logger

from supertable.demo.quickstart.defaults import super_name, organization, simple_name


role_manager = RoleManager(super_name=super_name, organization=organization, actor_role_name="superadmin")

# --- superadmin: already exists, look it up --------------------------------
#
# The superadmin role is minted once per SuperTable by RoleManager's bootstrap
# and is immutable: the *type* is reserved, so create_role refuses it. This
# used to create a second one, which worked only because nothing enforced the
# reservation — and left two superadmin roles where the model says there is
# exactly one.
superadmin_id = role_manager.get_superadmin_role_id()
logger.info(f"superadmin role (created at bootstrap) id: {superadmin_id}")

# --- admin -----------------------------------------------------------------
admin_data = {
    "role": "admin",
    "tables": {"*": {"columns": ["*"], "filters": ["*"]}},
}
admin_id = role_manager.create_role(admin_data)
logger.info(f"admin role created with id: {admin_id}")

# --- writer: full access to the facts table --------------------------------
writer_data = {
    "role": "writer",
    "tables": {simple_name: {"columns": ["*"], "filters": ["*"]}},
}
writer_id = role_manager.create_role(writer_data)
logger.info(f"writer role created with id: {writer_id}")

# --- meta: schema/stats only ----------------------------------------------
meta_data = {
    "role": "meta",
    "tables": {simple_name: {}},
}
meta_id = role_manager.create_role(meta_data)
logger.info(f"meta role created with id: {meta_id}")

# --- reader: column projection + row filter on facts -----------------------
reader_data = {
    "role": "reader",
    "tables": {
        simple_name: {
            "columns": ["day", "client", "value"],
            "filters": {"client": "client1"},
        }
    },
}
reader_id = role_manager.create_role(reader_data)
logger.info(f"reader role created with id: {reader_id}")

# --- list everything for inspection ---------------------------------------
all_roles = role_manager.list_roles()
logger.info(f"Total roles: {len(all_roles)}")
for role in all_roles:
    logger.info(role)
