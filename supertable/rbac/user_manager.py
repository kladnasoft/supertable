# supertable/rbac/user_manager.py

import json
import uuid
import time
from typing import Any, Dict, List, Optional

from supertable.config.defaults import logger
from supertable.redis_catalog import (
    RedisCatalog,
    RbacDecisionError,
    RbacDuplicateIdentityError,
    RbacIntegrityError,
    audit_rbac_manager_rejections,
    validate_username,
)
from supertable import redis_keys as RK
from supertable.audit.privileged import PrivilegedActionContext
from supertable.utils.diagnostic_redaction import safe_exception_type


def _safe_error_type(error: BaseException) -> str:
    """Return bounded exception metadata without rendering backend text."""

    return safe_exception_type(error)


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
    ):
        self.super_name = super_name
        self.organization = organization
        self._catalog = redis_catalog or RedisCatalog()
        self._init_user_storage()

    # ── bootstrap ───────────────────────────────────────────────────── #

    def _init_user_storage(self) -> None:
        """Ensure meta key exists and create the default superuser.

        Fast path: if the meta key already exists AND a superuser is
        present, skip entirely.
        """
        org, sup = self.organization, self.super_name
        # Fast path is valid only when the reserved user still carries the
        # current bootstrap superadmin role. Merely finding the username can
        # otherwise preserve a silently de-privileged recovery account.
        if self._catalog.r.exists(RK.rbac_user_meta(org, sup)):
            user_id = self._catalog.rbac_get_user_id_by_username(
                org, sup, "superuser",
            )
            superadmin_id = self._catalog.rbac_get_superadmin_role_id(org, sup)
            if user_id and superadmin_id:
                user = self._catalog.get_user_details(org, sup, user_id)
                if user and superadmin_id in user.get("roles", []):
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
                self._catalog.rbac_add_role_to_user(
                    org,
                    sup,
                    existing_id,
                    superadmin_role_id,
                    action_context=PrivilegedActionContext.system(
                        "Repair the bootstrap superuser assignment",
                        cause="rbac_bootstrap_repair",
                    ),
                )
                logger.info("Default superuser role repaired")
        else:
            try:
                user_id = self.create_user({
                    "username": "superuser",
                    "roles": [superadmin_role_id],
                }, action_context=PrivilegedActionContext.system(
                    "Create the bootstrap superuser",
                    cause="rbac_bootstrap",
                ))
                logger.info("Default superuser created")
            except Exception as e:
                logger.error(
                    "Default superuser creation failed; error_type=%s",
                    _safe_error_type(e),
                )
                # Bootstrap is a security boundary, not best-effort setup.
                # In particular, an unavailable mandatory audit ledger must
                # not be hidden behind a seemingly healthy UserManager.
                raise

    # ── CRUD ────────────────────────────────────────────────────────── #

    @audit_rbac_manager_rejections(
        action="user_create",
        resource_type="user",
        namespace="user",
    )
    def create_user(self, data: dict, *, action_context=None) -> str:
        """
        Create a new user and return its ``user_id`` (UUID).

        If a user with the same username already exists, returns the
        existing ``user_id`` (idempotent).
        """
        if not isinstance(data, dict):
            raise ValueError("user data must be an object")
        if "username" not in data:
            raise ValueError("username is required")

        org, sup = self.organization, self.super_name
        username = data["username"]
        # Validate up-front so callers get a clean error at the public
        # API boundary instead of after the uniqueness lookup. Catalog
        # layer re-checks on write as defense in depth.
        validate_username(username)

        roles = data.get("roles", [])
        if not isinstance(roles, list) or not all(
            isinstance(role_id, str) for role_id in roles
        ):
            raise ValueError("roles must be a list of role IDs")
        for role_id in roles:
            if not self._catalog.rbac_role_exists(org, sup, role_id):
                raise RbacDecisionError(
                    f"Role {role_id} does not exist",
                    outcome="no_change",
                    cause="resource_missing",
                    conditions=[{
                        "kind": "role_absent",
                        "role_id": role_id,
                    }],
                )

        existing_id = self._catalog.rbac_get_user_id_by_username(org, sup, username)
        if existing_id:
            existing = self._catalog.get_user_details(org, sup, existing_id)
            if (
                not existing
                or str(existing.get("username", "")).casefold()
                != username.casefold()
            ):
                raise RbacIntegrityError(
                    "RBAC username map and document are inconsistent"
                )
            if existing and sorted(existing.get("roles", [])) == sorted(roles):
                self._catalog.rbac_append_attempt(
                    org,
                    sup,
                    action="user_create",
                    resource_type="user",
                    resource_id=existing_id,
                    namespace="user",
                    outcome="no_change",
                    cause="idempotent_replay",
                    action_context=action_context,
                    conditions=[
                        {
                            "kind": "identity_claim",
                            "name": username,
                            "identity_id": existing_id,
                        },
                        {
                            "kind": "resource_fields",
                            "fields": {
                                "username": existing.get("username", ""),
                                "doc_version": existing.get("doc_version", "0"),
                            },
                        },
                        {
                            "kind": "user_roles_equal",
                            "role_ids": roles,
                            "version": existing.get("doc_version", "0"),
                        },
                    ],
                )
                return existing_id
            raise RbacDecisionError(
                f"Username {username!r} already exists with different content; "
                "use modify_user() explicitly",
                outcome="no_change",
                cause="identity_claim_unchanged",
                conditions=[{
                    "kind": "identity_claim",
                    "name": username,
                    "identity_id": existing_id,
                }],
            )

        user_id = uuid.uuid4().hex
        now_ms = str(int(time.time() * 1000))

        user_doc = {
            "user_id": user_id,
            "username": username,
            "roles": roles,
            "created_ms": now_ms,
            "modified_ms": now_ms,
        }

        try:
            self._catalog.rbac_create_user(
                org,
                sup,
                user_id,
                user_doc,
                action_context=action_context,
            )
        except RbacDuplicateIdentityError:
            # Reconcile a concurrent case-insensitive name claim without
            # weakening create semantics for different role assignments.
            winner_id = self._catalog.rbac_get_user_id_by_username(
                org, sup, username,
            )
            winner = (
                self._catalog.get_user_details(org, sup, winner_id)
                if winner_id else None
            )
            if (
                winner_id
                and winner
                and sorted(winner.get("roles", [])) == sorted(roles)
            ):
                return winner_id
            raise
        logger.debug("User created")
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

    @audit_rbac_manager_rejections(
        action="user_update",
        resource_type="user",
        namespace="user",
        resource_fields=("user_id",),
    )
    def modify_user(self, user_id: str, data: dict, *, action_context=None) -> None:
        """Modify an existing user.  Supported fields: ``username``, ``display_name``, ``roles``."""
        if not isinstance(data, dict):
            raise ValueError("user data must be an object")
        org, sup = self.organization, self.super_name
        existing = self._catalog.get_user_details(org, sup, user_id)
        if existing is None:
            raise RbacDecisionError(
                f"User {user_id} does not exist",
                outcome="no_change",
                cause="resource_missing",
                conditions=[{"kind": "resource_absent"}],
            )

        update_fields: Dict[str, Any] = {}

        if "roles" in data:
            roles = data["roles"]
            if not isinstance(roles, list) or not all(
                isinstance(role_id, str) for role_id in roles
            ):
                raise ValueError("roles must be a list of role IDs")
            for role_id in roles:
                if not self._catalog.rbac_role_exists(org, sup, role_id):
                    raise RbacDecisionError(
                        f"Role {role_id} does not exist",
                        outcome="no_change",
                        cause="resource_missing",
                        conditions=[{
                            "kind": "role_absent",
                            "role_id": role_id,
                        }],
                    )
            if existing.get("username", "").casefold() == "superuser":
                superadmin_id = self._catalog.rbac_get_superadmin_role_id(org, sup)
                if not superadmin_id or superadmin_id not in roles:
                    raise ValueError(
                        "The default superuser must retain the superadmin role"
                    )
            update_fields["roles"] = roles  # raw list — rbac_update_user serializes

        if "username" in data:
            new_username = data["username"]
            old_username = existing["username"]
            validate_username(new_username)
            if old_username.casefold() == "superuser" and new_username.casefold() != "superuser":
                raise ValueError("The default superuser cannot be renamed")
            if new_username.lower() != old_username.lower():
                # Validate format BEFORE the uniqueness lookup so the
                # error message is the most informative one. Catalog
                # layer re-checks on rbac_rename_user / rbac_update_user.
                conflicting_id = self._catalog.rbac_get_user_id_by_username(
                    org, sup, new_username,
                )
                if conflicting_id:
                    conflicting = self._catalog.get_user_details(
                        org, sup, conflicting_id,
                    )
                    if (
                        not conflicting
                        or str(conflicting.get("username", "")).casefold()
                        != new_username.casefold()
                    ):
                        raise RbacIntegrityError(
                            "RBAC username map and document are inconsistent"
                        )
                    raise RbacDecisionError(
                        f"Username '{new_username}' is already taken",
                        outcome="no_change",
                        cause="identity_claim_unchanged",
                        conditions=[{
                            "kind": "identity_claim",
                            "name": new_username,
                            "identity_id": conflicting_id,
                        }],
                    )
            update_fields["username"] = new_username

        if "display_name" in data:
            update_fields["display_name"] = str(data["display_name"]).strip()

        if update_fields:
            self._catalog.rbac_update_user(
                org,
                sup,
                user_id,
                update_fields,
                action_context=action_context,
            )
        else:
            self._catalog.rbac_append_attempt(
                org,
                sup,
                action="user_update",
                resource_type="user",
                resource_id=user_id,
                namespace="user",
                outcome="no_change",
                cause="empty_update",
                action_context=action_context,
                before_version=int(existing.get("doc_version", "0")),
                conditions=[{
                    "kind": "resource_fields",
                    "fields": {
                        "username": existing.get("username", ""),
                        "doc_version": existing.get("doc_version", "0"),
                    },
                }],
            )

    @audit_rbac_manager_rejections(
        action="user_delete",
        resource_type="user",
        namespace="user",
        resource_fields=("user_id",),
    )
    def delete_user(self, user_id: str, *, action_context=None) -> None:
        """Delete a user. The default superuser cannot be deleted."""
        org, sup = self.organization, self.super_name
        user = self._catalog.get_user_details(org, sup, user_id)
        if user is None:
            self._catalog.rbac_append_attempt(
                org,
                sup,
                action="user_delete",
                resource_type="user",
                resource_id=user_id,
                namespace="user",
                outcome="no_change",
                cause="resource_missing",
                action_context=action_context,
                conditions=[{"kind": "resource_absent"}],
            )
            error = ValueError(f"User {user_id} does not exist")
            self._catalog._rbac_mark_attempt_recorded(error)
            raise error
        if user.get("username", "").lower() == "superuser":
            raise ValueError("Superuser cannot be deleted")
        self._catalog.rbac_delete_user(
            org,
            sup,
            user_id,
            action_context=action_context,
        )
        logger.debug("User deleted")

    def list_users(self) -> List[Dict]:
        """List all user documents."""
        return self._catalog.get_users(self.organization, self.super_name)

    # ── role assignment helpers ─────────────────────────────────────── #

    @audit_rbac_manager_rejections(
        action="user_role_assign",
        resource_type="user_role_assignment",
        namespace="user",
        resource_fields=("user_id", "role_id"),
    )
    def add_role(self, user_id: str, role_id: str, *, action_context=None) -> bool:
        """Add a role to a user (atomic, idempotent)."""
        org, sup = self.organization, self.super_name
        if not self._catalog.rbac_role_exists(org, sup, role_id):
            self._catalog.rbac_append_attempt(
                org,
                sup,
                action="user_role_assign",
                resource_type="user_role_assignment",
                resource_id=f"{user_id}:{role_id}",
                namespace="user",
                outcome="no_change",
                cause="resource_missing",
                action_context=action_context,
                conditions=[{
                    "kind": "assignment_role_absent",
                    "user_id": user_id,
                    "role_id": role_id,
                }],
            )
            error = ValueError(f"Role {role_id} does not exist")
            self._catalog._rbac_mark_attempt_recorded(error)
            raise error
        return self._catalog.rbac_add_role_to_user(
            org,
            sup,
            user_id,
            role_id,
            action_context=action_context,
        )

    @audit_rbac_manager_rejections(
        action="user_role_remove",
        resource_type="user_role_assignment",
        namespace="user",
        resource_fields=("user_id", "role_id"),
    )
    def remove_role(self, user_id: str, role_id: str, *, action_context=None) -> bool:
        """Remove a role from a user (atomic)."""
        org, sup = self.organization, self.super_name
        user = self._catalog.get_user_details(org, sup, user_id)
        if user is None:
            self._catalog.rbac_append_attempt(
                org,
                sup,
                action="user_role_remove",
                resource_type="user_role_assignment",
                resource_id=f"{user_id}:{role_id}",
                namespace="user",
                outcome="no_change",
                cause="user_missing",
                action_context=action_context,
                conditions=[{
                    "kind": "assignment_user_absent",
                    "user_id": user_id,
                    "role_id": role_id,
                }],
            )
            error = ValueError(f"User {user_id} does not exist")
            self._catalog._rbac_mark_attempt_recorded(error)
            raise error
        if user.get("username", "").casefold() == "superuser":
            superadmin_id = self._catalog.rbac_get_superadmin_role_id(org, sup)
            if role_id == superadmin_id:
                raise ValueError(
                    "The superadmin role cannot be removed from the default superuser"
                )
        return self._catalog.rbac_remove_role_from_user(
            org,
            sup,
            user_id,
            role_id,
            action_context=action_context,
        )

    def get_or_create_default_user(self) -> Optional[str]:
        """Return the default superuser's user_id, creating it if needed."""
        org, sup = self.organization, self.super_name
        self._ensure_default_superuser()
        user_id = self._catalog.rbac_get_user_id_by_username(org, sup, "superuser")
        return user_id

    # Backward-compatible aliases ------------------------------------------

    def get_user_hash_by_name(self, user_name: str) -> Dict:
        """Deprecated: use ``get_user_by_name``."""
        return self.get_user_by_name(user_name)

    @audit_rbac_manager_rejections(
        action="user_role_remove",
        resource_type="user_role_assignment",
        namespace="user",
        resource_fields=("role_id",),
    )
    def remove_role_from_users(self, role_id: str, *, action_context=None) -> None:
        """Deprecated bulk mutation: use audited per-user or role deletion APIs."""
        raise RbacDecisionError(
            "remove_role_from_users() is unsupported; use RoleManager.delete_role() "
            "or remove_role() for one user",
            outcome="denied",
            cause="unsupported_bulk_operation",
        )
