# supertable/rbac/user_manager.py

import json
import uuid
import time
from typing import Dict, List, Optional

from supertable.config.defaults import logger
from supertable.redis_catalog import RedisCatalog, validate_username
from supertable import redis_keys as RK


class UserManager:
    """
    Business-logic layer for RBAC users.

    Each user has a **stable UUID** (``user_id``) that never changes.
    Usernames are mutable and always unique (case-insensitive).
    """

    def __init__(
        self,
        super_name: str,
        organization: str,
        redis_catalog: Optional[RedisCatalog] = None,
        actor_role_name: Optional[str] = None,
    ):
        """
        ``actor_role_name`` is the role on whose authority this instance
        administers users. Required to *mutate* — creating, modifying or
        deleting a user, and granting or revoking a role, all demand
        :attr:`Permission.RBAC` — and unnecessary to *read*.

        Checked in the mutating methods rather than here because
        ``__init__`` bootstraps: it creates the default ``superuser`` and
        binds the ``superadmin`` role to it, which must work before any
        actor exists. See :meth:`RoleManager.__init__` for the full
        reasoning, including the recursion it avoids.
        """
        self.super_name = super_name
        self.organization = organization
        self._catalog = redis_catalog or RedisCatalog()
        self._actor_role_name = actor_role_name
        self._init_user_storage()

    def _require_rbac(self, action: str) -> None:
        """Demand :attr:`Permission.RBAC` from this instance's actor.

        Fails closed when no actor was supplied: granting a role is how a
        principal gains every other permission, so an unauthenticated path
        to it would make the rest of the model advisory.
        """
        if not self._actor_role_name:
            raise PermissionError(
                f"Cannot {action}: no actor role was supplied. Construct "
                f"UserManager(..., actor_role_name=<role>) with a role that "
                f"holds the RBAC permission."
            )
        # Local import: access_control -> role_manager -> ... would cycle.
        from supertable.rbac.access_control import check_rbac_access

        check_rbac_access(
            super_name=self.super_name,
            organization=self.organization,
            role_name=self._actor_role_name,
        )

    # ── bootstrap ───────────────────────────────────────────────────── #

    def _init_user_storage(self) -> None:
        """Ensure meta key exists and create the default superuser.

        Fast path: if the meta key already exists AND a superuser is
        present, skip entirely.
        """
        org, sup = self.organization, self.super_name
        # Fast path: meta key exists + superuser exists → skip
        if self._catalog.r.exists(RK.rbac_user_meta(org, sup)):
            if self._catalog.rbac_get_user_id_by_username(org, sup, "superuser"):
                return

        self._catalog.rbac_init_user_meta(org, sup)
        self._ensure_default_superuser()

    def _ensure_default_superuser(self) -> None:
        """Create or repair the default superuser with the superadmin role."""
        org, sup = self.organization, self.super_name
        existing_id = self._catalog.rbac_get_user_id_by_username(org, sup, "superuser")

        superadmin_role_id = self._catalog.rbac_get_superadmin_role_id(org, sup)
        if not superadmin_role_id:
            logger.warning("Cannot ensure superuser: superadmin role not found")
            return

        if existing_id:
            user = self._catalog.get_user_details(org, sup, existing_id)
            if user and superadmin_role_id not in user.get("roles", []):
                self._catalog.rbac_add_role_to_user(org, sup, existing_id, superadmin_role_id)
                logger.info(f"Added superadmin role to existing superuser: {existing_id}")
        else:
            try:
                # _create_user: the bootstrap has no actor. It creates the
                # account that the superadmin role is bound to.
                user_id = self._create_user({
                    "username": "superuser",
                    "roles": [superadmin_role_id],
                })
                logger.info(f"Default superuser created: {user_id}")
            except Exception as e:
                logger.error(f"Failed to create default superuser: {e}")

    # ── CRUD ────────────────────────────────────────────────────────── #

    def create_user(self, data: dict) -> str:
        """
        Create a new user and return its ``user_id`` (UUID).

        If a user with the same username already exists, returns the
        existing ``user_id`` (idempotent).

        Requires :attr:`Permission.RBAC`.
        """
        self._require_rbac("create a user")
        return self._create_user(data)

    def _create_user(self, data: dict) -> str:
        """Create a user with no authorisation check.

        The body of :meth:`create_user`, split out so the bootstrap can
        create the default ``superuser`` before any actor exists.
        """
        if "username" not in data:
            raise ValueError("username is required")

        org, sup = self.organization, self.super_name
        username = data["username"]
        # Validate up-front so callers get a clean error at the public
        # API boundary instead of after the uniqueness lookup. Catalog
        # layer re-checks on write as defense in depth.
        validate_username(username)

        existing_id = self._catalog.rbac_get_user_id_by_username(org, sup, username)
        if existing_id:
            return existing_id

        roles = data.get("roles", [])
        for role_id in roles:
            if not self._catalog.rbac_role_exists(org, sup, role_id):
                raise ValueError(f"Role {role_id} does not exist")

        user_id = uuid.uuid4().hex
        now_ms = str(int(time.time() * 1000))

        user_doc = {
            "user_id": user_id,
            "username": username,
            "roles": roles,
            "created_ms": now_ms,
            "modified_ms": now_ms,
        }

        self._catalog.rbac_create_user(org, sup, user_id, user_doc)
        logger.debug(f"User created: {user_id} ({username})")
        return user_id

    def get_user(self, user_id: str) -> Dict:
        """Retrieve a user. Raises ``ValueError`` if not found."""
        user = self._catalog.get_user_details(self.organization, self.super_name, user_id)
        if user is None:
            raise ValueError(f"User {user_id} does not exist")
        return user

    def get_user_by_name(self, username: str) -> Dict:
        """Retrieve a user by username (case-insensitive)."""
        user_id = self._catalog.rbac_get_user_id_by_username(
            self.organization, self.super_name, username
        )
        if user_id is None:
            raise ValueError(f"User '{username}' does not exist")
        return self.get_user(user_id)

    def modify_user(self, user_id: str, data: dict) -> None:
        """Modify an existing user.  Supported fields: ``username``, ``display_name``, ``roles``.

        Requires :attr:`Permission.RBAC` — ``roles`` is assignable here, so
        this is a privilege-granting path as much as ``add_role`` is.
        """
        self._require_rbac("modify a user")
        org, sup = self.organization, self.super_name
        existing = self._catalog.get_user_details(org, sup, user_id)
        if existing is None:
            raise ValueError(f"User {user_id} does not exist")

        update_fields: Dict[str, Any] = {}

        if "roles" in data:
            roles = data["roles"]
            for role_id in roles:
                if not self._catalog.rbac_role_exists(org, sup, role_id):
                    raise ValueError(f"Role {role_id} does not exist")
            update_fields["roles"] = roles  # raw list — rbac_update_user serializes

        if "username" in data:
            new_username = data["username"]
            old_username = existing["username"]
            if new_username.lower() != old_username.lower():
                # Validate format BEFORE the uniqueness lookup so the
                # error message is the most informative one. Catalog
                # layer re-checks on rbac_rename_user / rbac_update_user.
                validate_username(new_username)
                if self._catalog.rbac_get_user_id_by_username(org, sup, new_username):
                    raise ValueError(f"Username '{new_username}' is already taken")
                self._catalog.rbac_rename_user(org, sup, user_id, old_username, new_username)
            update_fields["username"] = new_username

        if "display_name" in data:
            update_fields["display_name"] = str(data["display_name"]).strip()

        if update_fields:
            self._catalog.rbac_update_user(org, sup, user_id, update_fields)

    def delete_user(self, user_id: str) -> None:
        """Delete a user. The default superuser cannot be deleted.

        Requires :attr:`Permission.RBAC`.
        """
        self._require_rbac("delete a user")
        org, sup = self.organization, self.super_name
        user = self._catalog.get_user_details(org, sup, user_id)
        if user is None:
            raise ValueError(f"User {user_id} does not exist")
        if user.get("username", "").lower() == "superuser":
            raise ValueError("Superuser cannot be deleted")
        self._catalog.rbac_delete_user(org, sup, user_id)
        logger.debug(f"User deleted: {user_id}")

    def list_users(self) -> List[Dict]:
        """List all user documents."""
        return self._catalog.get_users(self.organization, self.super_name)

    # ── role assignment helpers ─────────────────────────────────────── #

    def add_role(self, user_id: str, role_id: str) -> bool:
        """Add a role to a user (atomic, idempotent).

        Requires :attr:`Permission.RBAC`. This is the single most powerful
        call in the subsystem: binding a role is how a principal acquires
        every permission that role holds, and the superadmin role's id is
        discoverable through the public ``get_superadmin_role_id()``. Ungated,
        ``add_role(me, get_superadmin_role_id())`` was a two-line takeover.
        """
        self._require_rbac("grant a role to a user")
        org, sup = self.organization, self.super_name
        if not self._catalog.rbac_role_exists(org, sup, role_id):
            raise ValueError(f"Role {role_id} does not exist")
        return self._catalog.rbac_add_role_to_user(org, sup, user_id, role_id)

    def remove_role(self, user_id: str, role_id: str) -> bool:
        """Remove a role from a user (atomic).

        Requires :attr:`Permission.RBAC`. Revocation is gated as well as
        granting: stripping an administrator's role is a denial-of-service
        on the lake's administration, and in the limit locks everyone out.
        """
        self._require_rbac("revoke a role from a user")
        return self._catalog.rbac_remove_role_from_user(
            self.organization, self.super_name, user_id, role_id
        )

    def get_or_create_default_user(self) -> Optional[str]:
        """Return the default superuser's user_id, creating it if needed.

        Deliberately ungated. It repairs the bootstrap account and nothing
        else: the username is fixed, the role is whatever bootstrap already
        minted, and no part of either comes from the caller. Gating it would
        also make it unusable in the one situation it exists for — a lake
        whose superuser is missing, where no actor can be resolved.
        """
        org, sup = self.organization, self.super_name
        user_id = self._catalog.rbac_get_user_id_by_username(org, sup, "superuser")
        if not user_id:
            self._ensure_default_superuser()
            user_id = self._catalog.rbac_get_user_id_by_username(org, sup, "superuser")
        return user_id

    # Backward-compatible aliases ------------------------------------------

    def get_user_hash_by_name(self, user_name: str) -> Dict:
        """Deprecated: use ``get_user_by_name``."""
        return self.get_user_by_name(user_name)

    def remove_role_from_users(self, role_id: str) -> None:
        """Deprecated: ``RoleManager.delete_role`` handles this atomically.

        Requires :attr:`Permission.RBAC`.
        """
        self._require_rbac("revoke a role from every user")
        org, sup = self.organization, self.super_name
        for uid in self._catalog.rbac_list_user_ids(org, sup):
            self._catalog.rbac_remove_role_from_user(org, sup, uid, role_id)