"""The permission model: what each role type is allowed to do.

Five permissions, five role types, and a mapping between them that is written
out in full. Nothing here is inferred.

WHY EVERY GRANT IS EXPLICIT
---------------------------
The previous mapping gave SUPERADMIN and ADMIN ``set(Permission)`` — every
permission that exists, including ones added later. That is how ``CREATE``
came to be granted to both without anyone deciding it should be: it was
declared in the enum and the set comprehension picked it up. A new permission
must now be granted deliberately, role by role, or it is granted to no one.

WHAT EACH PERMISSION GATES
--------------------------
``RBAC``
    Mint, modify and delete roles and users. Held by the two admin tiers.

``CONTROL``
    Drop the whole SuperTable. The only operation above WRITE, because it is
    the only one that destroys things a writer never created: every other
    table in the lake, and the RBAC configuration itself.

``WRITE``
    Everything a table's data owner does — insert, update and delete rows;
    configure per-table limits; compact; and **create or drop the table**.
    Writing to a name that does not exist creates it, so there is no separate
    create permission; and a role trusted to fill a table is trusted to drop
    it, so there is no separate drop permission either. The blast radius of
    WRITE is bounded by the role's table grants, which is what makes this
    safe: a writer can only destroy tables it was granted.

``READ``
    Read rows via SELECT, subject to the role's row filters and column masks.

``META``
    Read schemas, statistics, and table/SuperTable listings. No row data.
    Every role type holds it, so it is a floor rather than a tier: even a
    role that cannot read data can discover what data exists.

ON THE TRUST BOUNDARY
---------------------
These checks take a ``role_name`` string supplied by the caller. The library
has no sessions and no identity of its own, so a gate is exactly as strong as
the host's binding of authenticated principal to role name. If a host lets a
client choose its own ``role_name``, every gate here is advisory — including
the ones that predate this file. The model is worth stating precisely anyway:
a host that binds correctly gets the whole matrix enforced for free.
"""
from enum import Enum, auto
from typing import Dict, List, Optional, Set


class Permission(Enum):
    RBAC = auto()      # administer roles and users
    CONTROL = auto()   # drop tables, drop SuperTables
    WRITE = auto()     # insert/update/delete rows; create a table by writing
    READ = auto()      # SELECT rows
    META = auto()      # schemas, stats, listings — no row data


class RoleType(Enum):
    SUPERADMIN = "superadmin"  # everything
    ADMIN = "admin"            # everything — deliberately equal to SUPERADMIN
    WRITER = "writer"          # owns its granted tables: create, write, drop
    READER = "reader"          # read only, with row/column security applied
    META = "meta"              # metadata only (e.g. schema discovery)


#: Every grant, written out. See the module docstring for why this is not
#: ``set(Permission)`` for the admin tiers.
#:
#: SUPERADMIN and ADMIN hold identical sets, which is a decision rather than
#: an oversight: both tiers administer roles and users, and there is no
#: operation reserved to one. ``RoleType`` keeps both names because roles of
#: type ``admin`` already exist in the wild, and because a host may want the
#: distinction for its own bookkeeping even though this library draws none.
ROLE_PERMISSIONS: Dict[RoleType, Set[Permission]] = {
    RoleType.SUPERADMIN: {
        Permission.RBAC,
        Permission.CONTROL,
        Permission.WRITE,
        Permission.READ,
        Permission.META,
    },
    RoleType.ADMIN: {
        Permission.RBAC,
        Permission.CONTROL,
        Permission.WRITE,
        Permission.READ,
        Permission.META,
    },
    RoleType.WRITER: {
        Permission.WRITE,
        Permission.READ,
        Permission.META,
    },
    RoleType.READER: {
        Permission.READ,
        Permission.META,
    },
    RoleType.META: {
        Permission.META,
    },
}


def has_permission(role_type: RoleType, permission: Permission) -> bool:
    """Check if a given role type has the specified permission.

    Unknown role types resolve to the empty set, so an unrecognised type is
    denied everything rather than defaulting to anything.
    """
    allowed = ROLE_PERMISSIONS.get(role_type, set())
    return permission in allowed
