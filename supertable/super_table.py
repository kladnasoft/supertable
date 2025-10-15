# supertable/super_table.py

from __future__ import annotations

import os
from typing import Dict, Any

from supertable.config.defaults import logger
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.user_manager import UserManager
from supertable.storage.storage_factory import get_storage
from supertable.storage.storage_interface import StorageInterface
from supertable.redis_catalog import RedisCatalog


class SuperTable:
    """
    Minimal coordination object:
      - Ensures storage backend is available
      - Ensures Redis meta:root exists (no file-based meta)
      - Exposes helper to read heavy simple-table snapshots from MinIO/local via StorageInterface
    """

    def __init__(self, super_name: str, organization: str):
        self.identity = "super"
        self.super_name = super_name
        self.organization = organization

        # Storage for heavy JSON + parquet
        self.storage: StorageInterface = get_storage()

        # Redis catalog for meta & locking
        self.catalog = RedisCatalog()

        # Directories for data layout (still used for heavy JSON & data files)
        self.super_dir = os.path.join(self.organization, self.super_name, self.identity)
        logger.debug(f"super_dir: {self.super_dir}")

        # Fast path: if meta:root exists, don't touch storage
        if self.catalog.root_exists(self.organization, self.super_name):
            logger.debug(
                f"[SuperTable] Root exists in Redis for {self.organization}/{self.super_name}; "
                f"skipping storage mkdirs."
            )
            return

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
