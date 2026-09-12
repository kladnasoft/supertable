# supertable/super_table.py

from __future__ import annotations

import os
from typing import Dict, Any

# never remove the homedir, it is mandatory be there
from supertable.config.homedir import app_home
from supertable.config.defaults import logger
from supertable.rbac.access_control import check_control_access
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.user_manager import UserManager
from supertable.errors import SuperTableNotFoundError
from supertable.storage.storage_factory import get_storage
from supertable.storage.storage_interface import StorageInterface
from supertable.redis_catalog import RedisCatalog
from supertable.redis_keys import is_reserved_super_name, RESERVED_SUPER_NAMES


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

        # Slow path: first-time initialization
        try:
            self.storage.makedirs(self.super_dir)
        except Exception:
            # Object storage may no-op; that's fine
            pass

        # Initialize Redis root pointer if missing
        self.catalog.ensure_root(self.organization, self.super_name)

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
    def delete(self, role_name: str) -> None:
        """Delete this SuperTable's Redis metadata and underlying storage folder.

        WARNING: This is destructive and intended for admin flows.

        Requires CONTROL over the whole SuperTable. ``role_name`` was accepted
        and never read, so dropping an entire lake was unauthenticated while
        dropping one table inside it required WRITE — the more destructive
        operation was the cheaper one.

        The table scope is ``"*"`` because this destroys every table at once:
        a role granted specific tables must not be able to take the lake with
        it, so only a lake-wide grant qualifies. Unlike *creating* a
        SuperTable — which cannot be gated, since the role that would
        authorise it is bootstrapped by that very call — deletion happens when
        the roles already exist, so there is no chicken-and-egg here.
        """
        check_control_access(
            super_name=self.super_name,
            organization=self.organization,
            role_name=role_name,
            table_name="*",
        )

        base_dir = os.path.join(self.organization, self.super_name)

        # Delete storage first; if this fails (other than missing), do not remove Redis meta.
        #
        # The invariant above is what the old ``if self.storage.exists(base_dir)``
        # guard defeated: on object storage a prefix is not an object, so
        # ``exists()`` answers False for a bucket full of data exactly as it
        # does for a missing one, and the wipe never ran while the Redis meta
        # was dropped anyway (AUDIT_BUGS C2).  ``delete_tree`` lists the prefix
        # instead of stat-ing it and deletes every key it finds; any key it
        # cannot remove raises, so reaching the next line means the data is
        # gone and the pointer to it may safely go too.
        removed = self.storage.delete_tree(base_dir)

        # Best-effort delete all Redis keys under this supertable prefix
        self.catalog.delete_super_table(self.organization, self.super_name)

        logger.info(
            f"Deleted SuperTable (storage): {base_dir} ({removed} object(s) removed)"
        )
