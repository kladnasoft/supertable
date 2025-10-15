# [file name]: user_manager.py

import json
import hashlib
from datetime import datetime
from typing import Dict, List, Optional

from supertable.storage.storage_factory import get_storage
from supertable.locking import Locking
from supertable.config.defaults import logger
from supertable.redis_catalog import RedisCatalog, RedisOptions


class UserManager:
    def __init__(self, super_name: str, organization: str):
        """
        super_name: Base directory name.
        """
        self.module = "rbac"
        self.identity = "users"
        self.super_name = super_name
        self.organization = organization

        # Initialize Redis catalog
        self.redis_catalog = RedisCatalog()

        # For backward compatibility, keep storage for other operations if needed
        self.storage = get_storage()

        self.user_dir = f"{organization}/{super_name}/{self.module}/{self.identity}"
        logger.debug(f"user_dir: {self.user_dir}")
        self.locking = Locking(identity=self.super_name, working_dir=self.user_dir)
        self.init_user_storage()

    def _user_meta_key(self) -> str:
        return f"supertable:{self.organization}:{self.super_name}:meta:users:meta"

    def _user_key(self, user_hash: str) -> str:
        return f"supertable:{self.organization}:{self.super_name}:meta:users:{user_hash}"

    def _username_to_hash_key(self) -> str:
        return f"supertable:{self.organization}:{self.super_name}:meta:users:name_to_hash"

    def init_user_storage(self) -> None:
        """Initialize Redis storage for users"""
        meta_key = self._user_meta_key()

        # Check if meta exists, if not initialize with atomic operation
        if not self.redis_catalog.r.exists(meta_key):
            # Use Redis lock to prevent race conditions during initialization
            lock_token = self.redis_catalog.acquire_simple_lock(
                self.organization, self.super_name, "users_init", ttl_s=10, timeout_s=30
            )

            try:
                if lock_token:
                    # Double-check after acquiring lock
                    if not self.redis_catalog.r.exists(meta_key):
                        meta_data = {
                            "last_updated_ms": str(int(datetime.now().timestamp() * 1000)),
                            "version": "0",
                            "initialized": "true"
                        }
                        self.redis_catalog.r.hset(meta_key, mapping=meta_data)
                        logger.debug("Initialized user meta data in Redis")
            finally:
                if lock_token:
                    self.redis_catalog.release_simple_lock(
                        self.organization, self.super_name, "users_init", lock_token
                    )

        # Ensure default superuser exists (this will be called by each thread but should be idempotent)
        self._ensure_default_superuser()

    def _ensure_default_superuser(self) -> None:
        """Ensure default superuser exists with proper roles"""
        username_hash_key = self._username_to_hash_key()
        superuser_hash = self.redis_catalog.r.hget(username_hash_key, "superuser")

        if not superuser_hash:
            # Superuser doesn't exist, create it with proper roles
            self._create_default_superuser()
        else:
            # Superuser exists, ensure it has the superadmin role
            try:
                user_data = self.get_user(superuser_hash)
                roles = user_data.get("roles", [])
                if not roles:
                    # Add superadmin role to existing superuser
                    admin_role_hash = self._get_superadmin_role_hash()
                    if admin_role_hash and self._is_role_valid(admin_role_hash):
                        self.modify_user(superuser_hash, {"roles": [admin_role_hash]})
                        logger.info(f"Added superadmin role to existing superuser: {superuser_hash}")
            except Exception as e:
                logger.error(f"Error ensuring superuser roles: {e}")

    def _create_default_superuser(self) -> None:
        """Create the default superuser with proper roles"""
        try:
            # Get the superadmin role hash
            admin_role_hash = self._get_superadmin_role_hash()

            if admin_role_hash and self._is_role_valid(admin_role_hash):
                superuser_data = {
                    "username": "superuser",
                    "roles": [admin_role_hash]
                }
                superuser_hash = self.create_user(superuser_data)
                logger.info(f"Default superuser created with hash: {superuser_hash}, roles: {superuser_data['roles']}")
            else:
                logger.error(f"Cannot create superuser: superadmin role not found or invalid")
        except Exception as e:
            logger.error(f"Failed to create default superuser: {e}")

    def _get_superadmin_role_hash(self) -> Optional[str]:
        """Get the superadmin role hash with retry logic"""
        max_retries = 10
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Try to get roles from RoleManager
                from supertable.rbac.role_manager import RoleManager
                role_manager = RoleManager(super_name=self.super_name, organization=self.organization)
                admin_role_hash = role_manager.get_superadmin_role_hash()

                if admin_role_hash:
                    return admin_role_hash
                else:
                    logger.debug(f"Superadmin role not found, retrying... ({retry_count + 1}/{max_retries})")
                    import time
                    time.sleep(0.5)  # Wait for role initialization
                    retry_count += 1

            except Exception as e:
                logger.debug(f"Error getting superadmin role, retrying... ({retry_count + 1}/{max_retries}): {e}")
                import time
                time.sleep(0.5)
                retry_count += 1

        logger.warning("Superadmin role not found after retries")
        return None

    def _is_role_valid(self, role_hash: str) -> bool:
        """Check if a role hash is valid by checking if it exists in Redis"""
        try:
            role_key = f"supertable:{self.organization}:{self.super_name}:meta:roles:{role_hash}"
            return self.redis_catalog.r.exists(role_key)
        except Exception as e:
            logger.error(f"Error checking role validity {role_hash}: {e}")
            return False

    def _get_user_meta(self) -> Dict:
        """Get user metadata from Redis"""
        meta_key = self._user_meta_key()
        meta_data = self.redis_catalog.r.hgetall(meta_key)
        # Convert string values to proper types
        if meta_data:
            if 'version' in meta_data:
                meta_data['version'] = int(meta_data['version'])
            if 'last_updated_ms' in meta_data:
                meta_data['last_updated_ms'] = int(meta_data['last_updated_ms'])
        return meta_data or {}

    def _update_user_meta(self) -> None:
        """Update user metadata timestamp and version"""
        meta_key = self._user_meta_key()
        updates = {
            "last_updated_ms": str(int(datetime.now().timestamp() * 1000)),
        }
        # Increment version
        current_version = int(self.redis_catalog.r.hget(meta_key, "version") or 0)
        updates["version"] = str(current_version + 1)

        self.redis_catalog.r.hset(meta_key, mapping=updates)

    def _get_valid_roles(self) -> Dict[str, str]:
        """
        Retrieve valid roles from Redis roles storage.
        Returns a dictionary mapping role_hash -> role type.
        """
        try:
            # Use RoleManager to get roles instead of scanning Redis directly
            from supertable.rbac.role_manager import RoleManager
            role_manager = RoleManager(super_name=self.super_name, organization=self.organization)
            roles_list = role_manager.list_roles()

            valid_roles = {}
            for role_data in roles_list:
                role_hash = role_data.get("hash")
                role_type = role_data.get("role")
                if role_hash and role_type:
                    valid_roles[role_hash] = role_type

            return valid_roles
        except Exception as e:
            logger.error(f"Error getting valid roles from RoleManager: {e}")
            return {}

    def create_user(self, data: dict) -> str:
        """
        Create a user in Redis storage.
        """
        if "username" not in data:
            raise ValueError("username is required")

        username = data["username"]

        # Check if user with same username already exists
        username_hash_key = self._username_to_hash_key()
        existing_hash = self.redis_catalog.r.hget(username_hash_key, username.lower())
        if existing_hash:
            return existing_hash

        roles = data.get("roles", [])

        # Validate roles
        for role_hash in roles:
            if not self._is_role_valid(role_hash):
                raise ValueError(f"Role {role_hash} is not valid")

        # Create user hash
        base_user_data = {
            "username": username,
            "roles": json.dumps(roles, sort_keys=True),  # Store as JSON string for consistent hashing
        }
        json_str = json.dumps(base_user_data, sort_keys=True)
        user_hash = hashlib.md5(json_str.encode()).hexdigest()

        # Build complete user data
        user_data = {
            "username": username,
            "roles": json.dumps(roles),
            "created_ms": str(int(datetime.now().timestamp() * 1000)),
            "modified_ms": str(int(datetime.now().timestamp() * 1000)),
            "hash": user_hash
        }

        # Store user data in Redis Hash
        user_key = self._user_key(user_hash)
        self.redis_catalog.r.hset(user_key, mapping=user_data)

        # Update username to hash mapping
        self.redis_catalog.r.hset(username_hash_key, username.lower(), user_hash)

        # Update metadata
        self._update_user_meta()

        return user_hash

    def get_user(self, user_hash: str) -> Dict:
        """
        Retrieve a user configuration from Redis.
        """
        user_key = self._user_key(user_hash)
        if not self.redis_catalog.r.exists(user_key):
            raise ValueError(f"User {user_hash} does not exist")

        user_data = self.redis_catalog.r.hgetall(user_key)
        # Parse JSON fields
        if 'roles' in user_data:
            user_data['roles'] = json.loads(user_data['roles'])
        # Convert string timestamps to int
        if 'created_ms' in user_data:
            user_data['created_ms'] = int(user_data['created_ms'])
        if 'modified_ms' in user_data:
            user_data['modified_ms'] = int(user_data['modified_ms'])

        return user_data

    def get_user_hash_by_name(self, user_name: str) -> Dict:
        """
        Retrieve a user configuration by username.
        """
        username_hash_key = self._username_to_hash_key()
        user_hash = self.redis_catalog.r.hget(username_hash_key, user_name.lower())

        if user_hash is None:
            raise ValueError(f"User {user_name} does not exist")

        user_data = self.get_user(user_hash)
        return user_data

    def modify_user(self, user_hash: str, data: dict) -> None:
        """
        Modify an existing user in Redis.
        """
        user_key = self._user_key(user_hash)
        if not self.redis_catalog.r.exists(user_key):
            raise ValueError(f"User {user_hash} does not exist")

        user_data = self.redis_catalog.r.hgetall(user_key)

        if "roles" in data:
            roles = data["roles"]
            # Validate roles
            for role_hash in roles:
                if not self._is_role_valid(role_hash):
                    raise ValueError(f"Role {role_hash} is not valid")
            user_data["roles"] = json.dumps(roles)

        if "username" in data:
            new_username = data["username"]
            old_username = user_data["username"]

            # Update username mapping if username changed
            if new_username.lower() != old_username.lower():
                username_hash_key = self._username_to_hash_key()
                # Remove old mapping
                self.redis_catalog.r.hdel(username_hash_key, old_username.lower())
                # Add new mapping
                self.redis_catalog.r.hset(username_hash_key, new_username.lower(), user_hash)

            user_data["username"] = new_username

        user_data["modified_ms"] = str(int(datetime.now().timestamp() * 1000))

        # Update user data
        self.redis_catalog.r.hset(user_key, mapping=user_data)

        # Update metadata
        self._update_user_meta()

    def delete_user(self, user_hash: str) -> None:
        """
        Delete a user from Redis.
        The sysadmin user cannot be deleted.
        """
        user_key = self._user_key(user_hash)
        if not self.redis_catalog.r.exists(user_key):
            raise ValueError(f"User {user_hash} does not exist")

        # Get user data to check if it's sysadmin
        user_data = self.redis_catalog.r.hgetall(user_key)
        username = user_data.get("username", "")

        if username.lower() == "superuser":
            raise ValueError("Sysadmin user cannot be deleted")

        # Remove from username mapping
        username_hash_key = self._username_to_hash_key()
        self.redis_catalog.r.hdel(username_hash_key, username.lower())

        # Delete user data
        self.redis_catalog.r.delete(user_key)

        # Update metadata
        self._update_user_meta()

    def remove_role_from_users(self, role_hash: str) -> None:
        """
        Remove a role from all users in Redis.
        """
        # Scan all user keys
        pattern = f"supertable:{self.organization}:{self.super_name}:meta:users:*"
        user_keys = []
        cursor = 0
        while True:
            cursor, keys = self.redis_catalog.r.scan(cursor=cursor, match=pattern, count=1000)
            user_keys.extend([k for k in keys if k != self._user_meta_key() and k != self._username_to_hash_key()])
            if cursor == 0:
                break

        updated = False
        for user_key in user_keys:
            user_data = self.redis_catalog.r.hgetall(user_key)
            if 'roles' in user_data:
                roles = json.loads(user_data['roles'])
                if role_hash in roles:
                    roles.remove(role_hash)
                    user_data['roles'] = json.dumps(roles)
                    user_data['modified_ms'] = str(int(datetime.now().timestamp() * 1000))
                    self.redis_catalog.r.hset(user_key, mapping=user_data)
                    updated = True

        if updated:
            self._update_user_meta()

    def list_users(self) -> List[Dict]:
        """
        List all users from Redis.
        """
        pattern = f"supertable:{self.organization}:{self.super_name}:meta:users:*"
        user_keys = []
        cursor = 0
        while True:
            cursor, keys = self.redis_catalog.r.scan(cursor=cursor, match=pattern, count=1000)
            user_keys.extend([k for k in keys if k != self._user_meta_key() and k != self._username_to_hash_key()])
            if cursor == 0:
                break

        users = []
        for user_key in user_keys:
            try:
                user_data = self.redis_catalog.r.hgetall(user_key)
                if 'roles' in user_data:
                    user_data['roles'] = json.loads(user_data['roles'])
                if 'created_ms' in user_data:
                    user_data['created_ms'] = int(user_data['created_ms'])
                if 'modified_ms' in user_data:
                    user_data['modified_ms'] = int(user_data['modified_ms'])
                users.append(user_data)
            except Exception as e:
                logger.error(f"Error reading user data from {user_key}: {e}")
                continue

        return users

    def get_or_create_default_user(self) -> str:
        """
        Get the default superuser hash, creating it if it doesn't exist.
        This is useful for testing and default operations.
        """
        username_hash_key = self._username_to_hash_key()
        superuser_hash = self.redis_catalog.r.hget(username_hash_key, "superuser")

        if not superuser_hash:
            # Create default superuser
            self._create_default_superuser()
            superuser_hash = self.redis_catalog.r.hget(username_hash_key, "superuser")

        return superuser_hash