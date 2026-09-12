# supertable/rbac/role_manager.py

import uuid
from typing import Dict, List, Optional

from supertable.rbac.row_column_security import RowColumnSecurity
from supertable.config.defaults import logger
from supertable.redis_catalog import (
    RESERVED_ROLE_TYPE as _RESERVED_ROLE_TYPE,
    RedisCatalog,
    SAFE_ROLE_NAME_RE,
    validate_role_name,
)
from supertable import redis_keys as RK

try:
    from supertable.audit import emit as _audit_emit, EventCategory, Actions, Severity, make_detail
    _audit_available = True
except ImportError:
    _audit_available = False


# Role names the library owns.  ``superadmin`` is created at bootstrap in
# every SuperTable (:meth:`RoleManager._init_role_storage`), so before this
# reservation a tenant could ask for a role named "SuperAdmin" and — because
# ``create_role`` returned the *existing* id on a name collision — be handed
# the bootstrap superadmin's id, with the requested type and table grants
# silently discarded (S11 / M12).  Matching is case-insensitive because the
# name→id index lowercases on write.
RESERVED_ROLE_NAMES = frozenset({"superadmin"})


def _check_reserved_role_name(role_name: Optional[str]) -> None:
    """Raise ``ValueError`` if *role_name* is one the library reserves."""
    if not role_name:
        return
    if role_name.strip().lower() in RESERVED_ROLE_NAMES:
        raise ValueError(
            f"Role name {role_name!r} is reserved by SuperTable and cannot be "
            f"created or assigned by a tenant."
        )


#: The role *type* the library owns, as distinct from the name above.
#:
#: Reserving the name alone left the type open, and the type is what the
#: enforcement path reads: ``access_control`` resolves a role, takes
#: ``role_info["role"]``, and for ``superadmin`` returns an empty view set —
#: no row filters, no column masks. So a role created as
#: ``{"role": "superadmin", "role_name": "quarterly_report_viewer"}`` passed
#: the name check and then had its own ``tables`` restriction discarded at
#: read time. There is exactly one superadmin role and ``_init_role_storage``
#: makes it; nothing else may mint one (S11).
#:
#: Re-exported from the catalog layer, which is where the invariants are
#: enforced — ``RedisCatalog`` is public, so a check that lived only here was
#: skippable by importing it directly. Checks in this module remain as the
#: user-facing contract (clearer errors, earlier in the call), not as the
#: only line of defence.
RESERVED_ROLE_TYPE = _RESERVED_ROLE_TYPE


def _check_reserved_role_type(role_type: Optional[str]) -> None:
    """Raise ``ValueError`` if *role_type* is the reserved superadmin type."""
    if not role_type:
        return
    if str(role_type).strip().lower() == RESERVED_ROLE_TYPE:
        raise ValueError(
            f"Role type {RESERVED_ROLE_TYPE!r} is reserved by SuperTable. It "
            f"is created once at initialisation and cannot be created, "
            f"assigned, or promoted to by a tenant."
        )


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
        actor_role_name: Optional[str] = None,
    ):
        """
        ``actor_role_name`` is the role on whose authority this instance
        administers roles. It is required to *mutate* — ``create_role``,
        ``update_role`` and ``delete_role`` all demand
        :attr:`Permission.RBAC`, which only the admin tiers hold — and
        unnecessary to *read*, so the access-control layer can keep building
        throwaway instances to resolve a role without supplying one.

        The check is in the mutating methods rather than here on purpose.
        Two reasons, both structural:

        * ``__init__`` bootstraps. It mints this SuperTable's ``superadmin``
          role, so a constructor that demanded RBAC could never run the
          first time — the role that would authorise it is created by the
          call needing it.
        * ``check_rbac_access`` resolves the actor by building a
          ``RoleManager``. If the constructor validated, that inner instance
          would validate too, and so on without bound.
        """
        self.super_name = super_name
        self.organization = organization
        self._catalog = redis_catalog or RedisCatalog()
        self._actor_role_name = actor_role_name
        self._init_role_storage()

    def _require_rbac(self, action: str) -> None:
        """Demand :attr:`Permission.RBAC` from this instance's actor.

        Fails closed when no actor was supplied. The alternative — treating
        a missing actor as "unrestricted" — would mean the gate protected
        only callers who had already opted into being checked.
        """
        if not self._actor_role_name:
            raise PermissionError(
                f"Cannot {action}: no actor role was supplied. Construct "
                f"RoleManager(..., actor_role_name=<role>) with a role that "
                f"holds the RBAC permission."
            )
        # Local import: access_control imports this module, so a top-level
        # import here would be circular.
        from supertable.rbac.access_control import check_rbac_access

        check_rbac_access(
            super_name=self.super_name,
            organization=self.organization,
            role_name=self._actor_role_name,
        )

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
                    # _create_role, not create_role: the bootstrap has no
                    # actor to be authorised by — it is what creates the
                    # first role able to authorise anything.
                    role_id = self._create_role(sysadmin_data, allow_reserved=True)
                    logger.info(f"Default superadmin role created: {role_id}")
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

    def create_role(self, data: dict, allow_reserved: bool = False) -> str:
        """
        Create a new role and return its ``role_id`` (UUID).

        ``data`` must contain at least ``role`` (a RoleType string)
        and ``tables`` (a dict of per-table definitions).
        ``role_name`` is optional but must be unique (case-insensitive)
        when provided; it enables name-based lookups.

        A name collision raises ``ValueError`` unless the stored role is
        *identical* to the one requested — that keeps genuine retries
        idempotent while refusing to hand back a role the caller did not ask
        for.  Returning a different role silently discarded the requested type
        and grants, and with ``superadmin`` reachable by name it returned the
        bootstrap superadmin id to anyone who asked (S11 / M12).

        ``allow_reserved`` is for the library's own bootstrap only; tenant
        callers must not set it.

        Requires :attr:`Permission.RBAC` from this instance's
        ``actor_role_name``.

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
        self._require_rbac("create a role")
        return self._create_role(data, allow_reserved=allow_reserved)

    def _create_role(self, data: dict, allow_reserved: bool = False) -> str:
        """Create a role with no authorisation check.

        The body of :meth:`create_role`, split out so the bootstrap can mint
        the first ``superadmin`` role — which by definition happens before
        any role exists to authorise it.
        """
        org, sup = self.organization, self.super_name
        role_name = data.get("role_name")

        # Validate role_name format up-front so callers get a clean error
        # at the public API boundary instead of after RCS prep work. The
        # catalog layer re-checks on write, but that's defense in depth —
        # this is the user-facing contract.
        validate_role_name(role_name)
        if not allow_reserved:
            _check_reserved_role_name(role_name)
            # The type too, not just the name. Reserving one without the other
            # left the enforcement-relevant half open — see RESERVED_ROLE_TYPE.
            _check_reserved_role_type(data.get("role"))

        rcs = RowColumnSecurity(**{k: v for k, v in data.items() if k != "role_name"})
        rcs.prepare()

        # If role_name given, check uniqueness. Idempotent only for an
        # identical request (same role type and same grants); anything else is
        # a collision the caller has to resolve.
        if role_name:
            existing_id = self._catalog.rbac_get_role_id_by_name(org, sup, role_name)
            if existing_id:
                existing = self._catalog.get_role_details(org, sup, existing_id) or {}
                if existing.get("content_hash") == rcs.content_hash:
                    return existing_id
                raise ValueError(
                    f"Role name '{role_name}' is already taken by role "
                    f"{existing_id} with different content. Use update_role to "
                    f"change it, or pick another name."
                )

        role_id = uuid.uuid4().hex

        role_doc = rcs.to_json()
        role_doc["role_id"] = role_id
        role_doc["content_hash"] = rcs.content_hash
        if role_name:
            role_doc["role_name"] = role_name

        self._catalog.rbac_create_role(org, sup, role_id, role_doc)
        logger.debug(f"Role created: {role_id} ({rcs.role.value})")

        # Creating a role was the only RBAC mutation that emitted nothing,
        # while update and delete both did — so an audit trail could show a
        # grant being widened or revoked but not granted in the first place.
        _audit_rbac(org, sup, Actions.ROLE_CREATE, role_id,
                    role_name=role_name or "", role_type=rcs.role.value)
        return role_id

    def update_role(self, role_id: str, data: dict) -> str:
        """
        Update a role's content in-place.  Returns the new content_hash.

        Only the fields present in ``data`` are changed.
        ``role_id`` remains stable.  If ``role_name`` is being changed,
        validates format and checks uniqueness.

        Requires :attr:`Permission.RBAC`.
        """
        self._require_rbac("update a role")
        org, sup = self.organization, self.super_name
        existing = self._catalog.get_role_details(org, sup, role_id)
        if not existing:
            raise ValueError(f"Role {role_id} does not exist")

        # Handle role_name rename
        new_name = data.get("role_name")
        old_name = existing.get("role_name", "")
        if new_name is not None and new_name != old_name:
            # Validate format (same rule as create_role; catalog re-checks
            # on write as defense in depth).
            validate_role_name(new_name)
            _check_reserved_role_name(new_name)
            # Check uniqueness
            if new_name:
                conflicting_id = self._catalog.rbac_get_role_id_by_name(org, sup, new_name)
                if conflicting_id and conflicting_id != role_id:
                    raise ValueError(f"Role name '{new_name}' is already taken by role {conflicting_id}")

        # The superadmin type is immutable in both directions.
        #
        # Promotion was the same hole as create_role: nothing stopped
        # ``update_role(some_reader_id, {"role": "superadmin"})``. It was also
        # invisible, because ``rbac_update_role`` rewrote the document without
        # moving the role between the ``roles:type:doc:*`` index sets, so a
        # promoted role stayed filed under its old type — effective at
        # enforcement, absent from ``get_superadmin_role_id()``. The index is
        # now moved atomically with the document, but the promotion itself is
        # refused here regardless: one superadmin role, minted by bootstrap.
        #
        # Demotion mattered just as much, because it made "the superadmin role
        # cannot be deleted" bypassable in two steps: demote it to reader, at
        # which point ``delete_role``'s type check no longer matches and the
        # delete succeeds. Guarding only the delete left the lake reachable
        # with no superadmin at all and no way to mint a replacement.
        new_role_type = data.get("role")
        old_role_type = str(existing.get("role") or "").strip().lower()
        if new_role_type is not None:
            requested = str(new_role_type).strip().lower()
            if requested != old_role_type:
                _check_reserved_role_type(requested)
                if old_role_type == RESERVED_ROLE_TYPE:
                    raise ValueError(
                        "The superadmin role's type cannot be changed. It is "
                        "created at initialisation and is the only role that "
                        "can restore access if others are misconfigured."
                    )

        # A role document with no ``tables`` field has no grant — updating an
        # unrelated field must not invent one.  The old fallback here was the
        # wildcard-everything entry, so any update to a tableless document was
        # a silent promotion to full access (the C4 substitution, second site).
        merged = {
            "role": data.get("role", existing.get("role")),
            "tables": data.get("tables", existing.get("tables", {})),
        }

        rcs = RowColumnSecurity(**merged)
        rcs.prepare()

        update_fields = rcs.to_json()
        update_fields["content_hash"] = rcs.content_hash

        # If role_name changed, update the name_to_id mapping
        if new_name is not None and new_name != old_name:
            update_fields["role_name"] = new_name
            name_key = RK.rbac_rolename_to_id(org, sup)
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

        Requires :attr:`Permission.RBAC`.
        """
        self._require_rbac("delete a role")
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
