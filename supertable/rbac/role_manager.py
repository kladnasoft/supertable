# supertable/rbac/role_manager.py

import uuid
from typing import Dict, List, Optional

from supertable.rbac.row_column_security import (
    RowColumnSecurity,
    canonicalize_role_tables,
)
from supertable.config.defaults import logger
from supertable.redis_catalog import (
    RedisCatalog,
    RbacDecisionError,
    RbacDuplicateIdentityError,
    RbacIntegrityError,
    SAFE_ROLE_NAME_RE,
    audit_rbac_manager_rejections,
    validate_role_name,
)
from supertable import redis_keys as RK
from supertable.audit.privileged import PrivilegedActionContext


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
        if self._catalog.r.exists(RK.rbac_role_meta(org, sup)):
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
                    self.create_role(
                        sysadmin_data,
                        action_context=PrivilegedActionContext.system(
                            "Create the bootstrap superadmin role",
                            cause="rbac_bootstrap",
                        ),
                    )
                    logger.info("Default superadmin role created")
            finally:
                if lock_token:
                    self._catalog.release_simple_lock(
                        self.organization, self.super_name, "roles_init", lock_token,
                    )

    # ── CRUD ────────────────────────────────────────────────────────── #

    # Re-exported for backwards compat with callers that referenced the
    # old class attribute. The single source of truth lives on the
    # ``redis_catalog`` layer (see :data:`SAFE_ROLE_NAME_RE`) so direct
    # catalog writers can't bypass the rule. See :func:`validate_role_name`
    # for the canonical check.
    _SAFE_ROLE_NAME_RE = SAFE_ROLE_NAME_RE

    @staticmethod
    def _prepare_role_content(
        data: dict,
        *,
        default_if_empty: bool = True,
    ) -> RowColumnSecurity:
        """Validate the public role-policy payload and return canonical content."""
        if not isinstance(data, dict):
            raise ValueError("role data must be an object")
        unknown = set(data) - {"role", "role_name", "tables"}
        if unknown:
            raise ValueError(f"Unknown role fields: {sorted(str(k) for k in unknown)}")
        if "role" not in data:
            raise ValueError("role is required")
        tables = canonicalize_role_tables(
            data.get("tables"),
            default_if_empty=default_if_empty,
            allow_legacy_list=False,
        )
        rcs = RowColumnSecurity(role=data["role"], tables=tables)
        rcs.tables = tables
        rcs.create_content_hash()
        return rcs

    @audit_rbac_manager_rejections(
        action="role_create",
        resource_type="role",
        namespace="role",
    )
    def create_role(
        self,
        data: dict,
        *,
        action_context=None,
    ) -> str:
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
        if not isinstance(data, dict):
            raise ValueError("role data must be an object")
        org, sup = self.organization, self.super_name
        role_name = data.get("role_name")

        # Validate role_name format up-front so callers get a clean error
        # at the public API boundary instead of after RCS prep work. The
        # catalog layer re-checks on write, but that's defense in depth —
        # this is the user-facing contract.
        validate_role_name(role_name)

        # Validate before the idempotent-name fast path.  Otherwise a caller
        # could submit a malformed or over-budget policy and receive a success
        # response merely because the name already existed.
        rcs = self._prepare_role_content(data)

        # If role_name given, check uniqueness (idempotent: return existing)
        if role_name:
            existing_id = self._catalog.rbac_get_role_id_by_name(org, sup, role_name)
            if existing_id:
                existing = self._catalog.get_role_details(org, sup, existing_id)
                if (
                    not existing
                    or str(existing.get("role_name", "")).casefold()
                    != role_name.casefold()
                ):
                    raise RbacIntegrityError(
                        "RBAC role name map and document are inconsistent"
                    )
                if (
                    existing
                    and existing.get("role") == rcs.role.value
                    and existing.get("content_hash") == rcs.content_hash
                ):
                    self._catalog.rbac_append_attempt(
                        org,
                        sup,
                        action="role_create",
                        resource_type="role",
                        resource_id=existing_id,
                        namespace="role",
                        outcome="no_change",
                        cause="idempotent_replay",
                        action_context=action_context,
                        conditions=[
                            {
                                "kind": "identity_claim",
                                "name": role_name,
                                "identity_id": existing_id,
                            },
                            {
                                "kind": "resource_fields",
                                "fields": {
                                    "role": existing.get("role", ""),
                                    "content_hash": existing.get(
                                        "content_hash", ""
                                    ),
                                    "doc_version": existing.get("doc_version", "0"),
                                },
                            },
                        ],
                    )
                    return existing_id
                raise RbacDecisionError(
                    f"Role name {role_name!r} already exists with different content; "
                    "use update_role() explicitly",
                    outcome="no_change",
                    cause="identity_claim_unchanged",
                    conditions=[{
                        "kind": "identity_claim",
                        "name": role_name,
                        "identity_id": existing_id,
                    }],
                )

        role_id = uuid.uuid4().hex

        role_doc = rcs.to_json()
        role_doc["role_id"] = role_id
        role_doc["content_hash"] = rcs.content_hash
        if role_name:
            role_doc["role_name"] = role_name

        try:
            self._catalog.rbac_create_role(
                org,
                sup,
                role_id,
                role_doc,
                action_context=action_context,
            )
        except RbacDuplicateIdentityError:
            # A concurrent named create can win after the optimistic lookup
            # above but before the catalog's atomic name claim.  Preserve the
            # public idempotency contract only when that winner has exactly
            # the content we already validated; never turn a different policy
            # into a successful create response.
            if role_name:
                winner_id = self._catalog.rbac_get_role_id_by_name(
                    org, sup, role_name,
                )
                winner = (
                    self._catalog.get_role_details(org, sup, winner_id)
                    if winner_id else None
                )
                if (
                    winner
                    and winner.get("role") == rcs.role.value
                    and winner.get("content_hash") == rcs.content_hash
                ):
                    return winner_id
            raise
        logger.debug("Role created")
        return role_id

    @audit_rbac_manager_rejections(
        action="role_update",
        resource_type="role",
        namespace="role",
        resource_fields=("role_id",),
    )
    def update_role(
        self,
        role_id: str,
        data: dict,
        *,
        action_context=None,
    ) -> str:
        """
        Update a role's content in-place.  Returns the new content_hash.

        Only the fields present in ``data`` are changed.
        ``role_id`` remains stable.  If ``role_name`` is being changed,
        validates format and checks uniqueness.
        """
        if not isinstance(data, dict):
            raise ValueError("role data must be an object")
        unknown = set(data) - {"role", "role_name", "tables"}
        if unknown:
            raise ValueError(f"Unknown role fields: {sorted(str(k) for k in unknown)}")

        org, sup = self.organization, self.super_name
        existing = self._catalog.get_role_details(org, sup, role_id)
        if not existing:
            raise RbacDecisionError(
                f"Role {role_id} does not exist",
                outcome="no_change",
                cause="resource_missing",
                conditions=[{"kind": "resource_absent"}],
            )

        # Handle role_name rename
        new_name = data.get("role_name")
        old_name = existing.get("role_name", "")
        if new_name is not None and new_name != old_name:
            # Validate format (same rule as create_role; catalog re-checks
            # on write as defense in depth).
            validate_role_name(new_name)
            # Check uniqueness
            if new_name:
                conflicting_id = self._catalog.rbac_get_role_id_by_name(org, sup, new_name)
                if conflicting_id and conflicting_id != role_id:
                    conflicting = self._catalog.get_role_details(
                        org, sup, conflicting_id,
                    )
                    if (
                        not conflicting
                        or str(conflicting.get("role_name", "")).casefold()
                        != new_name.casefold()
                    ):
                        raise RbacIntegrityError(
                            "RBAC role name map and document are inconsistent"
                        )
                    raise RbacDecisionError(
                        f"Role name '{new_name}' is already taken by role {conflicting_id}",
                        outcome="no_change",
                        cause="identity_claim_unchanged",
                        conditions=[{
                            "kind": "identity_claim",
                            "name": new_name,
                            "identity_id": conflicting_id,
                        }],
                    )

        default_tables = {"*": {"columns": ["*"], "filters": ["*"]}}

        merged = {
            "role": data.get("role", existing.get("role")),
            "tables": data.get("tables", existing.get("tables", default_tables)),
        }

        if existing.get("role") == "superadmin" and merged["role"] != "superadmin":
            raise ValueError("A superadmin role cannot be demoted.")
        if (
            existing.get("role") == "superadmin"
            and str(old_name).casefold() == "superadmin"
            and new_name is not None
            and str(new_name).casefold() != "superadmin"
        ):
            raise ValueError("The bootstrap superadmin role cannot be renamed.")

        rcs = self._prepare_role_content(
            merged,
            # Preserve the historical explicit update API where tables={}
            # means wildcard, but never widen an already persisted empty
            # (fail-closed) policy during an unrelated partial update.
            default_if_empty="tables" in data,
        )

        update_fields = rcs.to_json()
        update_fields["content_hash"] = rcs.content_hash

        # RedisCatalog updates the role document, name map, type indexes, and
        # RBAC version in one transaction.  Keeping the rename in that boundary
        # avoids a window where the name resolves to content that was not yet
        # committed (or no longer resolves after a failed document update).
        if new_name is not None and new_name != old_name:
            update_fields["role_name"] = new_name

        self._catalog.rbac_update_role(
            org,
            sup,
            role_id,
            update_fields,
            action_context=action_context,
        )
        logger.debug("Role updated")

        assert rcs.content_hash is not None
        return rcs.content_hash

    @audit_rbac_manager_rejections(
        action="role_delete",
        resource_type="role",
        namespace="role",
        resource_fields=("role_id",),
    )
    def delete_role(self, role_id: str, *, action_context=None) -> bool:
        """
        Delete a role and atomically strip it from all users.

        The superadmin role cannot be deleted.
        """
        org, sup = self.organization, self.super_name
        existing = self._catalog.get_role_details(org, sup, role_id)
        if existing and existing.get("role") == "superadmin":
            raise ValueError("The superadmin role cannot be deleted.")
        result = self._catalog.rbac_delete_role(
            org,
            sup,
            role_id,
            action_context=action_context,
        )
        if result:
            logger.debug("Role deleted")
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
