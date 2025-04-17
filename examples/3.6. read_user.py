
from supertable.super_table import SuperTable
from supertable.rbac.user_manager import UserManager
from examples.defaults import super_name, organization


# ---------- USER OPERATIONS ----------
# Initialize the UserManager with the same base directory.
user_manager = UserManager(super_name=super_name, organization=organization)

res = user_manager.get_user_hash_by_name("superuser")
print(res)