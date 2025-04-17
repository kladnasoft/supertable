from enum import Enum, auto

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


