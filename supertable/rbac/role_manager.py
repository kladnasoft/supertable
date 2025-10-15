# [file name]: role_manager.py

import os
import json
import hashlib
from datetime import datetime
from typing import Dict, List, Optional

from supertable.rbac.row_column_security import RowColumnSecurity
from supertable.storage.storage_factory import get_storage
from supertable.locking import Locking
from supertable.config.defaults import logger
from supertable.redis_catalog import RedisCatalog


class RoleManager:
    def __init__(self, super_name: str, organization: str):
        """
        super_name: Base directory name where roles will be stored in Redis.
        """
        self.module = "rbac"
        self.identity = "roles"
        self.super_name = super_name
        self.organization = organization

        # Initialize Redis catalog
        self.redis_catalog = RedisCatalog()

        # For backward compatibility
        self.storage = get_storage()

        self.role_dir = os.path.join(self.organization, self.super_name, self.module, self.identity)
        logger.debug(f"role_dir: {self.role_dir}")
        self.locking = Locking(identity=self.super_name, working_dir=self.role_dir)
        self.init_role_storage()

    def _role_meta_key(self) -> str:
        return f"supertable:{self.organization}:{self.super_name}:meta:roles:meta"

    def _role_key(self, role_hash: str) -> str:
        return f"supertable:{self.organization}:{self.super_name}:meta:roles:{role_hash}"

    def _role_type_to_hash_key(self) -> str:
        return f"supertable:{self.organization}:{self.super_name}:meta:roles:type_to_hash"

    def init_role_storage(self) -> None:
        """Initialize Redis storage for roles - this should be called first"""
        meta_key = self._role_meta_key()

        # Check if meta exists, if not initialize with atomic operation
        if not self.redis_catalog.r.exists(meta_key):
            # Use Redis lock to prevent race conditions during initialization
            lock_token = self.redis_catalog.acquire_simple_lock(
                self.organization, self.super_name, "roles_init", ttl_s=10, timeout_s=30
            )

            try:
                if lock_token:
                    # Double-check after acquiring lock
                    if not self.redis_catalog.r.exists(meta_key):
                        meta_data = {
                            "version": "0",
                            "last_updated_ms": str(int(datetime.now().timestamp() * 1000)),
                            "roles_initialized": "true"
                        }
                        self.redis_catalog.r.hset(meta_key, mapping=meta_data)

                        # Create the default admin role
                        sysadmin_data = {
                            "role": "superadmin",
                            "tables": ["*"],  # Allow access to all tables
                            "columns": ["*"],  # Allow access to all columns
                            "filters": ["*"]  # No row filters
                        }
                        sysadmin_hash = self.create_role(sysadmin_data)
                        logger.info(f"Default sysadmin role created with hash: {sysadmin_hash}")
            finally:
                if lock_token:
                    self.redis_catalog.release_simple_lock(
                        self.organization, self.super_name, "roles_init", lock_token
                    )

    def _get_role_meta(self) -> Dict:
        """Get role metadata from Redis"""
        meta_key = self._role_meta_key()
        meta_data = self.redis_catalog.r.hgetall(meta_key)
        # Convert string values to proper types
        if meta_data:
            if 'version' in meta_data:
                meta_data['version'] = int(meta_data['version'])
            if 'last_updated_ms' in meta_data:
                meta_data['last_updated_ms'] = int(meta_data['last_updated_ms'])
        return meta_data or {}

    def _update_role_meta(self) -> None:
        """Update role metadata timestamp and version"""
        meta_key = self._role_meta_key()
        updates = {
            "last_updated_ms": str(int(datetime.now().timestamp() * 1000)),
        }
        # Increment version
        current_version = int(self.redis_catalog.r.hget(meta_key, "version") or 0)
        updates["version"] = str(current_version + 1)

        self.redis_catalog.r.hset(meta_key, mapping=updates)

    def create_role(self, data: dict) -> str:
        """
        Create a role in Redis storage.
        """
        # Create a role (with row/column security) from the provided data.
        role_data = RowColumnSecurity(**data)
        role_data.prepare()

        # Check if role already exists
        role_key = self._role_key(role_data.hash)
        if self.redis_catalog.r.exists(role_key):
            return role_data.hash

        # Prepare role content for Redis storage
        role_content = role_data.to_json()
        role_content["hash"] = role_data.hash

        # Convert all values to strings for Redis Hash storage
        redis_data = {}
        for key, value in role_content.items():
            if isinstance(value, (list, dict)):
                redis_data[key] = json.dumps(value)
            elif isinstance(value, bool):
                redis_data[key] = str(value).lower()
            else:
                redis_data[key] = str(value)

        # Store role data in Redis Hash
        self.redis_catalog.r.hset(role_key, mapping=redis_data)

        # Update role type to hash mapping
        role_type_key = self._role_type_to_hash_key()
        self.redis_catalog.r.sadd(f"{role_type_key}:{role_data.role.value}", role_data.hash)

        # Update metadata
        self._update_role_meta()

        return role_data.hash

    def delete_role(self, role_hash: str) -> bool:
        """
        Delete a role from Redis storage.
        """
        role_key = self._role_key(role_hash)
        if not self.redis_catalog.r.exists(role_key):
            return False

        # Get role data to remove from type mapping
        role_data = self.redis_catalog.r.hgetall(role_key)
        role_type = role_data.get("role")

        if role_type:
            role_type_key = self._role_type_to_hash_key()
            self.redis_catalog.r.srem(f"{role_type_key}:{role_type}", role_hash)

        # Delete role data
        self.redis_catalog.r.delete(role_key)

        # Update metadata
        self._update_role_meta()

        return True

    def get_role(self, role_hash: str) -> Dict:
        """
        Retrieve a role configuration from Redis.
        """
        role_key = self._role_key(role_hash)
        if not self.redis_catalog.r.exists(role_key):
            return {}

        role_data = self.redis_catalog.r.hgetall(role_key)

        # Parse JSON fields back to Python objects
        for field in ["tables", "columns", "filters"]:
            if field in role_data:
                try:
                    role_data[field] = json.loads(role_data[field])
                except json.JSONDecodeError:
                    # Keep as string if not valid JSON
                    pass

        return role_data

    def list_roles(self) -> List[Dict]:
        """
        List all roles from Redis storage.
        """
        pattern = f"supertable:{self.organization}:{self.super_name}:meta:roles:*"
        role_keys = []
        cursor = 0
        while True:
            cursor, keys = self.redis_catalog.r.scan(cursor=cursor, match=pattern, count=1000)
            role_keys.extend([k for k in keys if k != self._role_meta_key() and 'type_to_hash' not in k])
            if cursor == 0:
                break

        roles_list = []
        for role_key in role_keys:
            try:
                role_data = self.get_role(role_key.split(":")[-1])  # Extract hash from key
                if role_data:
                    roles_list.append(role_data)
            except Exception as e:
                logger.error(f"Error reading role data from {role_key}: {e}")
                continue

        return roles_list

    def get_roles_by_type(self, role_type: str) -> List[Dict]:
        """
        Get all roles of a specific type.
        """
        role_type_key = self._role_type_to_hash_key()
        role_hashes = self.redis_catalog.r.smembers(f"{role_type_key}:{role_type}")

        roles = []
        for role_hash in role_hashes:
            role_data = self.get_role(role_hash)
            if role_data:
                roles.append(role_data)

        return roles

    def get_superadmin_role_hash(self) -> Optional[str]:
        """
        Get the superadmin role hash.
        """
        role_type_key = self._role_type_to_hash_key()
        role_hashes = self.redis_catalog.r.smembers(f"{role_type_key}:superadmin")

        if role_hashes:
            return next(iter(role_hashes), None)
        return None

