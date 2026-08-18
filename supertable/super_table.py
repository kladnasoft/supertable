# supertable/super_table.py

from __future__ import annotations

import os
from typing import Dict, Any, Optional

from supertable.config.defaults import logger
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.user_manager import UserManager
from supertable.errors import SuperTableNotFoundError
from supertable.storage.storage_factory import get_storage
from supertable.storage.storage_interface import StorageInterface
from supertable.redis_catalog import RedisCatalog
from supertable.redis_keys import is_reserved_super_name, RESERVED_SUPER_NAMES
from supertable.rbac.access_control import resolve_role_access_context
from supertable.rbac.permissions import Permission, RoleType


class SuperTable:
    """
    Minimal coordination object:
      - Ensures storage backend is available
      - Ensures Redis meta:root exists (no file-based meta)
      - Exposes helper to read heavy simple-table snapshots from MinIO/local via StorageInterface

    Reserved supertable names (any underscore-wrapped name like
    ``_foo_`` — matched by ``redis_keys.SENTINEL_RE``) are rejected
    up-front. Lakes live under ``supertable:{org}:lakes:{sup}``, so
    structurally they could not collide with the org-level ``system:``
    namespace anyway — the sentinel-pattern reservation is defence in
    depth against future system labels.

    Args:
        super_name: Name of the supertable (organization-scoped).
        organization: Organization (tenant) name.
        create_if_missing: When True (default), bootstrap the supertable
            (storage mkdir, Redis ``meta:root``, RBAC scaffolding) if it
            does not exist. When False, raise
            ``SuperTableNotFoundError`` instead. Read-side callers
            (``DataReader``, ``MetaReader``, ``DataEstimator``) pass
            ``False`` so a missing supertable surfaces as an error
            instead of being silently materialized as a side effect of
            constructing the Python object.
    """

    def __init__(
        self,
        super_name: str,
        organization: str,
        *,
        create_if_missing: bool = True,
    ):
        if is_reserved_super_name(super_name):
            raise ValueError(
                f"SuperTable name {super_name!r} is reserved and cannot be created. "
                f"Reserved names: {sorted(RESERVED_SUPER_NAMES)}"
            )

        self.identity = "super"
        self.super_name = super_name
        self.organization = organization

        # Storage for heavy JSON + parquet
        self.storage: StorageInterface = get_storage()

        # Redis catalog for meta & locking
        self.catalog = RedisCatalog()

        deletion_guard = getattr(
            type(self.catalog), "check_deletion_intent_absent", None,
        )
        if callable(deletion_guard):
            self.catalog.check_deletion_intent_absent(
                self.organization,
                self.super_name,
            )

        # Directories for data layout (still used for heavy JSON & data files)
        self.super_dir = os.path.join(self.organization, self.super_name, self.identity)

        # Fast path: if meta:root exists, don't touch storage
        if self.catalog.root_exists(self.organization, self.super_name):
            return

        # Read-only opt-out: refuse to bootstrap as a side effect. This
        # is the guarantee that lets ``DataReader`` / ``MetaReader`` open
        # a session against a missing name and get a clean, named error
        # back instead of silently creating an empty supertable.
        if not create_if_missing:
            raise SuperTableNotFoundError(organization, super_name)

        self.init_super_table()

        # Initialize RBAC scaffolding
        RoleManager(super_name=self.super_name, organization=self.organization)
        UserManager(super_name=self.super_name, organization=self.organization)

    # ------------------------------------------------------------------ init
    def init_super_table(self) -> None:
        """
        Initialize super table:
          * If Redis meta:root already exists -> skip any folder checks/creations.
          * Otherwise, create the base folder (best-effort) and bootstrap Redis meta:root.
        """

        token = self.catalog.acquire_namespace_lock(
            self.organization, self.super_name, ttl_s=30, timeout_s=60,
        )
        if not token:
            raise TimeoutError(
                f"Could not acquire namespace creation lock for "
                f"{self.organization}/{self.super_name}"
            )
        try:
            # Check before either fast-path return: stale root metadata behind
            # a terminal tombstone must never reopen a deleted namespace.
            self.catalog.check_initialization_allowed(
                self.organization,
                self.super_name,
                namespace_token=token,
            )
            if self.catalog.root_exists(self.organization, self.super_name):
                return
            # The structural lock is acquired before the first storage write,
            # so an initializer paused here cannot write a directory marker
            # after a concurrent verified namespace deletion.
            try:
                self.storage.makedirs(self.super_dir)
            except Exception:
                # Object storage may no-op; that's fine
                pass

            self.catalog.ensure_root(
                self.organization,
                self.super_name,
                namespace_token=token,
            )
        finally:
            self.catalog.release_namespace_lock(
                self.organization, self.super_name, token,
            )

    # ------------------------------------------------------------------ heavy JSON read
    def read_simple_table_snapshot(self, simple_table_path: str) -> Dict[str, Any]:
        """
        Read the **heavy** simple-table snapshot JSON from storage (MinIO/local).
        """
        if not simple_table_path or not self.storage.exists(simple_table_path):
            raise FileNotFoundError(f"Simple table snapshot not found: {simple_table_path}")
        if self.storage.size(simple_table_path) == 0:
            raise ValueError(f"Simple table snapshot is empty: {simple_table_path}")
        return self.storage.read_json(simple_table_path)


    # ------------------------------------------------------------------ delete
    def delete(self, role_name) -> str:
        """Delete this SuperTable's data metadata and storage folder.

        WARNING: This is destructive and intended for admin flows.

        RBAC role/user state is intentionally retained.  It is security
        control data and may only be removed through its dedicated mandatory
        audit boundary; recreating the same SuperTable therefore cannot reset
        or silently widen its prior access policy.
        """
        return self._delete_with_intent(role_name=role_name)

    def recover_delete(
            self,
            role_name: str,
            *,
            intent_id: str,
            confirm_previous_owner_stopped: bool = False,
    ) -> str:
        """Resume an abandoned namespace deletion after liveness proof."""
        return self._delete_with_intent(
            role_name=role_name,
            recovery_intent_id=intent_id,
            confirm_previous_owner_stopped=confirm_previous_owner_stopped,
        )

    def _delete_with_intent(
            self,
            *,
            role_name: str,
            recovery_intent_id: Optional[str] = None,
            confirm_previous_owner_stopped: bool = False,
    ) -> str:
        # Deleting the namespace also deletes every child table.  A scoped
        # ADMIN could otherwise authorize the parent through ``*`` and erase a
        # child that has an exact ``access: deny`` override.  There is no root
        # structural lock that can make a per-child preflight race-free, so the
        # parent destructive operation is reserved for the trusted SUPERADMIN
        # control plane.  SimpleTable.delete remains table-policy aware.
        context = resolve_role_access_context(
            super_name=self.super_name,
            organization=self.organization,
            role_name=role_name,
            permission=Permission.CONTROL,
            label="delete this SuperTable",
        )
        if context.role_type is not RoleType.SUPERADMIN:
            raise PermissionError(
                "Only SUPERADMIN can delete an entire SuperTable namespace."
            )

        base_dir = os.path.join(self.organization, self.super_name)
        namespace_token = self.catalog.acquire_namespace_lock(
            self.organization, self.super_name, ttl_s=30, timeout_s=60,
        )
        if not namespace_token:
            raise TimeoutError(
                f"Could not acquire deletion fence for "
                f"{self.organization}/{self.super_name}"
            )
        leaf_tokens: Dict[str, str] = {}
        stage_tokens: Dict[str, str] = {}
        try:
            if recovery_intent_id is None:
                intent = self.catalog.begin_namespace_deletion(
                    self.organization,
                    self.super_name,
                    namespace_token=namespace_token,
                )
            else:
                intent = self.catalog.recover_namespace_deletion(
                    self.organization,
                    self.super_name,
                    expected_intent_id=recovery_intent_id,
                    namespace_token=namespace_token,
                    confirm_previous_owner_stopped=(
                        confirm_previous_owner_stopped
                    ),
                )
            intent_id = intent.get("intent_id") if isinstance(intent, dict) else None
            if not intent_id:
                raise RuntimeError("Catalog returned an invalid deletion intent")
            logger.info(
                "[deletion] SuperTable cleanup started for %s/%s; "
                "deletion_intent_id=%s; recovery=%s",
                self.organization,
                self.super_name,
                intent_id,
                recovery_intent_id is not None,
            )

            # The namespace fence prevents new child initialization and makes
            # snapshot commit Lua fail closed. Drain every already-published
            # child's auto-renewed writer lock before touching storage.
            leaf_marker = "meta:leaf:doc:"
            names = sorted({
                key.rsplit(leaf_marker, 1)[-1]
                for key in self.catalog.scan_leaf_keys(
                    self.organization,
                    self.super_name,
                    resolve_replica=False,
                )
                if leaf_marker in key
            })
            for name in names:
                token = self.catalog.acquire_simple_lock(
                    self.organization,
                    self.super_name,
                    name,
                    ttl_s=30,
                    timeout_s=60,
                )
                if not token:
                    raise TimeoutError(
                        f"Could not drain writer for "
                        f"{self.organization}/{self.super_name}/{name}"
                    )
                leaf_tokens[name] = token

            # A first-time writer owns a leaf lock before it has any leaf to
            # enumerate. Discover live lock keys as well and require two stable
            # complete scans after the durable parent intent. New entrants now
            # fail their pre-I/O intent check; pre-intent owners are drained.
            stable_leaf_scans = 0
            leaf_scan_rounds = 0
            while stable_leaf_scans < 2:
                leaf_scan_rounds += 1
                if leaf_scan_rounds > 10_000:
                    raise RuntimeError(
                        "Table-writer drain exceeded its stability bound"
                    )
                names = self.catalog.scan_leaf_lock_names(
                    self.organization,
                    self.super_name,
                )
                missing = sorted(set(names).difference(leaf_tokens))
                if not missing:
                    stable_leaf_scans += 1
                    continue
                stable_leaf_scans = 0
                if len(leaf_tokens) + len(missing) > 10_000:
                    raise RuntimeError(
                        "Table-writer drain exceeded its key bound"
                    )
                for name in missing:
                    token = self.catalog.acquire_simple_lock(
                        self.organization,
                        self.super_name,
                        name,
                        ttl_s=30,
                        timeout_s=60,
                    )
                    if not token:
                        raise TimeoutError(
                            f"Could not drain writer for "
                            f"{self.organization}/{self.super_name}/{name}"
                        )
                    leaf_tokens[name] = token

            # Stage uploads also perform object-store I/O while holding their
            # own renewable lease. Enumerate lock keys rather than only the
            # staging index: a first-time creator owns a lock before it has any
            # metadata to list. Two complete stable scans close discovery churn
            # after the durable namespace intent has made every newly-entering
            # writer fail before storage I/O.
            stable_stage_scans = 0
            stage_scan_rounds = 0
            while stable_stage_scans < 2:
                stage_scan_rounds += 1
                if stage_scan_rounds > 10_000:
                    raise RuntimeError(
                        "Stage-writer drain exceeded its stability bound"
                    )
                names = self.catalog.scan_stage_lock_names(
                    self.organization,
                    self.super_name,
                )
                missing = sorted(set(names).difference(stage_tokens))
                if not missing:
                    stable_stage_scans += 1
                    continue
                stable_stage_scans = 0
                if len(stage_tokens) + len(missing) > 10_000:
                    raise RuntimeError(
                        "Stage-writer drain exceeded its key bound"
                    )
                for name in missing:
                    token = self.catalog.acquire_stage_lock(
                        self.organization,
                        self.super_name,
                        name,
                        ttl_s=30,
                        timeout_s=60,
                    )
                    if not token:
                        raise TimeoutError(
                            f"Could not drain staging writer for "
                            f"{self.organization}/{self.super_name}/{name}"
                        )
                    stage_tokens[name] = token

            # Object-store prefixes normally have no exact marker object. The
            # backend operation drains and verifies the full prefix while no
            # existing or newly-created table can publish into it.
            self.storage.delete_prefix(base_dir)

            # Preserve live fence keys throughout the SCAN deletion. Once the
            # catalog root is gone, finally releasing them cannot admit a stale
            # writer: its root/leaf recheck or fenced commit fails.
            self.catalog.delete_super_table(
                self.organization,
                self.super_name,
                namespace_token=namespace_token,
                intent_id=intent_id,
            )
            if recovery_intent_id is not None:
                self.catalog.clear_namespace_deletion_tombstone(
                    self.organization,
                    self.super_name,
                    expected_intent_id=intent_id,
                    namespace_token=namespace_token,
                    confirm_previous_owner_stopped=(
                        confirm_previous_owner_stopped
                    ),
                )
        finally:
            for name, token in reversed(list(stage_tokens.items())):
                self.catalog.release_stage_lock(
                    self.organization, self.super_name, name, token,
                )
            for name, token in reversed(list(leaf_tokens.items())):
                self.catalog.release_simple_lock(
                    self.organization, self.super_name, name, token,
                )
            self.catalog.release_namespace_lock(
                self.organization, self.super_name, namespace_token,
            )
        return str(intent_id)

    @classmethod
    def recover_pending_delete(
            cls,
            *,
            organization: str,
            super_name: str,
            role_name: str,
            intent_id: str,
            confirm_previous_owner_stopped: bool = False,
    ) -> str:
        """Recover a namespace after its root was already removed."""
        table = cls.__new__(cls)
        table.identity = "super"
        table.super_name = super_name
        table.organization = organization
        table.storage = get_storage()
        table.catalog = RedisCatalog()
        table.super_dir = os.path.join(organization, super_name, "super")
        return table.recover_delete(
            role_name,
            intent_id=intent_id,
            confirm_previous_owner_stopped=confirm_previous_owner_stopped,
        )
