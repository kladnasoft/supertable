# supertable/rbac/role_manager.py

import uuid
from typing import Dict, List, Optional

from supertable.rbac.row_column_security import RowColumnSecurity
from supertable.config.defaults import logger
from supertable.redis_catalog import RedisCatalog

try:
    from supertable.audit import emit as _audit_emit, EventCategory, Actions, Severity, make_detail
    _audit_available = True
except ImportError:
    _audit_available = False


def _audit_rbac(organization: str, super_name: str, action, resource_id: str,
                severity=None, **detail_kwargs) -> None:
    """Emit an RBAC audit event.  Never raises."""
    if not _audit_available:
        return
    try:
        _audit_emit(
            category=EventCategory.RBAC_CHANGE,
            action=action,
            organization=organization,
            super_name=super_name,
            resource_type="role",
            resource_id=resource_id,
            severity=severity or Severity.WARNING,
            detail=make_detail(**detail_kwargs),
        )
    except Exception:
        pass


class RoleManager:
    """
    Business-logic layer for RBAC roles.

    Each role has a **stable UUID** (``role_id``) that never changes.
    Role *content* (tables, columns, filters) can be updated in-place
    via ``update_role``; all users referencing the role instantly see the
    new permissions.
    """

    def __init__(
        self,
        super_name: str,
        organization: str,
        redis_catalog: Optional[RedisCatalog] = None,
    ):
        self.super_name = super_name
        self.organization = organization
        self._catalog = redis_catalog or RedisCatalog()
        self._init_role_storage()

    # ── bootstrap ───────────────────────────────────────────────────── #

    def _init_role_storage(self) -> None:
        """Ensure meta key exists and create the default superadmin role.

        Fast path: if the meta key already exists AND a superadmin role
        is present, skip entirely.  This avoids 2-3 Redis calls per
        RoleManager instantiation in the common case.
        """
        org, sup = self.organization, self.super_name
        # Fast path: meta key exists → roles are initialized
        if self._catalog.r.exists(f"supertable:{org}:{sup}:rbac:roles:meta"):
            if self._catalog.rbac_get_superadmin_role_id(org, sup):
                return

        self._catalog.rbac_init_role_meta(org, sup)

        if not self._catalog.rbac_get_superadmin_role_id(org, sup):
            lock_token = self._catalog.acquire_simple_lock(
                self.organization, self.super_name, "roles_init", ttl_s=10, timeout_s=30,
            )
            try:
                if lock_token and not self._catalog.rbac_get_superadmin_role_id(
                    self.organization, self.super_name
                ):
                    sysadmin_data = {
                        "role": "superadmin",
                        "role_name": "superadmin",
                        "tables": {"*": {"columns": ["*"], "filters": ["*"]}},
                    }
                    role_id = self.create_role(sysadmin_data)
                    logger.info(f"Default superadmin role created: {role_id}")
            finally:
                if lock_token:
                    self._catalog.release_simple_lock(
                        self.organization, self.super_name, "roles_init", lock_token,
                    )

    # ── CRUD ────────────────────────────────────────────────────────── #

    _SAFE_ROLE_NAME_RE = __import__("re").compile(r"^[A-Za-z_][A-Za-z0-9_\- ]{0,126}$")

    def create_role(self, data: dict) -> str:
        """
        Create a new role and return its ``role_id`` (UUID).

        ``data`` must contain at least ``role`` (a RoleType string)
        and ``tables`` (a dict of per-table definitions).
        ``role_name`` is optional but must be unique (case-insensitive)
        when provided; it enables name-based lookups.

        Table definition format::

            {
                "role": "reader",
                "role_name": "sales_analyst",
                "tables": {
                    "orders": {"columns": ["order_id", "amount"], "filters": [...]},
                    "customers": {"columns": ["*"], "filters": ["*"]}
                }
            }
        """
        org, sup = self.organization, self.super_name
        role_name = data.get("role_name")

        # Validate role_name format
        if role_name and not self._SAFE_ROLE_NAME_RE.fullmatch(role_name):
            raise ValueError(
                f"Invalid role_name: {role_name!r}. Must be 1-127 characters, "
                "start with a letter or underscore, contain only letters, digits, "
                "underscores, hyphens, and spaces."
            )

        # If role_name given, check uniqueness (idempotent: return existing)
        if role_name:
            existing_id = self._catalog.rbac_get_role_id_by_name(org, sup, role_name)
            if existing_id:
                return existing_id

        rcs = RowColumnSecurity(**{k: v for k, v in data.items() if k != "role_name"})
        rcs.prepare()

        role_id = uuid.uuid4().hex

        role_doc = rcs.to_json()
        role_doc["role_id"] = role_id
        role_doc["content_hash"] = rcs.content_hash
        if role_name:
            role_doc["role_name"] = role_name

        self._catalog.rbac_create_role(org, sup, role_id, role_doc)
        logger.debug(f"Role created: {role_id} ({rcs.role.value})")
        return role_id

    def update_role(self, role_id: str, data: dict) -> str:
        """
        Update a role's content in-place.  Returns the new content_hash.

        Only the fields present in ``data`` are changed.
        ``role_id`` remains stable.  If ``role_name`` is being changed,
        validates format and checks uniqueness.
        """
        org, sup = self.organization, self.super_name
        existing = self._catalog.get_role_details(org, sup, role_id)
        if not existing:
            raise ValueError(f"Role {role_id} does not exist")

        # Handle role_name rename
        new_name = data.get("role_name")
        old_name = existing.get("role_name", "")
        if new_name is not None and new_name != old_name:
            # Validate format
            if new_name and not self._SAFE_ROLE_NAME_RE.fullmatch(new_name):
                raise ValueError(
                    f"Invalid role_name: {new_name!r}. Must be 1-127 characters, "
                    "start with a letter or underscore, contain only letters, digits, "
                    "underscores, hyphens, and spaces."
                )
            # Check uniqueness
            if new_name:
                conflicting_id = self._catalog.rbac_get_role_id_by_name(org, sup, new_name)
                if conflicting_id and conflicting_id != role_id:
                    raise ValueError(f"Role name '{new_name}' is already taken by role {conflicting_id}")

        default_tables = {"*": {"columns": ["*"], "filters": ["*"]}}

        merged = {
            "role": data.get("role", existing.get("role")),
            "tables": data.get("tables", existing.get("tables", default_tables)),
        }

        rcs = RowColumnSecurity(**merged)
        rcs.prepare()

        update_fields = rcs.to_json()
        update_fields["content_hash"] = rcs.content_hash

        # If role_name changed, update the name_to_id mapping
        if new_name is not None and new_name != old_name:
            update_fields["role_name"] = new_name
            name_key = f"supertable:{org}:{sup}:rbac:roles:name_to_id"
            pipe = self._catalog.r.pipeline()
            if old_name:
                pipe.hdel(name_key, old_name.lower())
            if new_name:
                pipe.hset(name_key, new_name.lower(), role_id)
            pipe.execute()

        self._catalog.rbac_update_role(org, sup, role_id, update_fields)
        logger.debug(f"Role updated: {role_id}")

        _audit_rbac(org, sup, Actions.ROLE_UPDATE, role_id,
                     role_name=new_name or old_name, role_type=merged.get("role", ""))

        return rcs.content_hash

    def delete_role(self, role_id: str) -> bool:
        """
        Delete a role and atomically strip it from all users.

        The superadmin role cannot be deleted.
        """
        org, sup = self.organization, self.super_name
        existing = self._catalog.get_role_details(org, sup, role_id)
        if existing and existing.get("role") == "superadmin":
            raise ValueError("The superadmin role cannot be deleted.")
        role_name = existing.get("role_name", "") if existing else ""
        result = self._catalog.rbac_delete_role(org, sup, role_id)
        if result:
            logger.debug(f"Role deleted: {role_id}")
            _audit_rbac(org, sup, Actions.ROLE_DELETE, role_id,
                         severity=Severity.CRITICAL, role_name=role_name)
        return result

    def get_role(self, role_id: str) -> Dict:
        """Retrieve a role configuration.  Returns ``{}`` if not found."""
        return self._catalog.get_role_details(self.organization, self.super_name, role_id) or {}

    def get_role_by_name(self, role_name: str) -> Dict:
        """Retrieve a role by its unique name (case-insensitive).

        Returns ``{}`` if not found.
        """
        role_id = self._catalog.rbac_get_role_id_by_name(
            self.organization, self.super_name, role_name,
        )
        if not role_id:
            return {}
        return self.get_role(role_id)

    def list_roles(self) -> List[Dict]:
        """List all role documents."""
        return self._catalog.get_roles(self.organization, self.super_name)

    def get_roles_by_type(self, role_type: str) -> List[Dict]:
        """Get all roles of a specific type."""
        roles = []
        for rid in self._catalog.rbac_get_role_ids_by_type(
            self.organization, self.super_name, role_type
        ):
            role = self._catalog.get_role_details(self.organization, self.super_name, rid)
            if role:
                roles.append(role)
        return roles

    def get_superadmin_role_id(self) -> Optional[str]:
        """Return the first superadmin role_id, or None."""
        return self._catalog.rbac_get_superadmin_role_id(self.organization, self.super_name)

    # Backward-compatible alias
    def get_superadmin_role_hash(self) -> Optional[str]:
        """Deprecated: use ``get_superadmin_role_id``."""
        return self.get_superadmin_role_id()
