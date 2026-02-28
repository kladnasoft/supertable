from enum import Enum, auto
from typing import Dict, List, Optional

class Permission(Enum):
    CONTROL = auto()
    CREATE = auto()
    WRITE = auto()
    READ = auto()
    META = auto()


class RoleType(Enum):
    SUPERADMIN = "superadmin" # Can do anything: read, write, delete, etc.
    ADMIN = "admin"  # Can do anything: read, write, delete, etc.
    WRITER = "writer"  # Can read and write.
    READER = "reader"  # Read only with extra row/column security.
    META = "meta"  # Read only (e.g., statistical data).

# Mapping each RoleType to its allowed permissions
ROLE_PERMISSIONS = {
    RoleType.SUPERADMIN: set(Permission),  # All permissions assigned to admin.
    RoleType.ADMIN: set(Permission),  # All permissions assigned to admin.
    RoleType.WRITER: {Permission.META, Permission.READ, Permission.WRITE},
    RoleType.READER: {Permission.META, Permission.READ},
    RoleType.META: {Permission.META},
}


def has_permission(role_type: RoleType, permission: Permission) -> bool:
    """Check if a given role type has the specified permission."""
    allowed = ROLE_PERMISSIONS.get(role_type, set())
    return permission in allowed


def get_role_permissions(role_name: Optional[str] = None) -> Optional[Dict[str, List[str]]]:
    """Return role(s) and their permissions as ``{role_name: [perm, …]}``.

    * If *role_name* is provided, returns that single role's permissions
      or ``None`` if the role does not exist.
    * If *role_name* is omitted / ``None``, returns all roles.
    """
    if role_name is not None:
        try:
            role_type = RoleType(role_name.lower())
        except ValueError:
            return None
        perms = ROLE_PERMISSIONS.get(role_type, set())
        return {role_type.value: sorted(p.name for p in perms)}

    return {
        rt.value: sorted(p.name for p in perms)
        for rt, perms in ROLE_PERMISSIONS.items()
    }