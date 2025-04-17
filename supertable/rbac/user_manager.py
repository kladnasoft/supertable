import os
import json
import hashlib
from datetime import datetime

from supertable.storage.storage_factory import get_storage
from supertable.locking import Locking
from supertable.config.defaults import logger

class UserManager:
    def __init__(self, super_name: str, organization: str):
        """
        super_name: Base directory name.
        """
        self.module = "rbac"
        self.identity = "users"
        self.super_name = super_name
        self.organization = organization

        # Choose the storage backend via a factory.
        self.storage = get_storage()

        # Users will be stored in a dedicated directory.
        self.user_dir = os.path.join(self.organization, self.super_name, self.module, self.identity)
        self.user_meta_path = os.path.join(self.organization, self.super_name, "_users.json")
        logger.debug(f"user_dir: {self.user_dir}")
        logger.debug(f"user_meta: {self.user_meta_path}")
        self.locking = Locking(identity=self.super_name, working_dir=self.user_dir)
        self.init_user_storage()

    def init_user_storage(self) -> None:
        self.storage.makedirs(self.user_dir)
        meta_data = None
        if self.storage.exists(self.user_meta_path):
            try:
                meta_data = self.storage.read_json(self.user_meta_path)
            except Exception as e:
                logger.error(f"Warning: Users meta file invalid, reinitializing: {e}")
        if meta_data is None:
            meta_data = {
                "last_updated_ms": int(datetime.now().timestamp() * 1000),
                "version": 0,
                "users": {}  # Mapping user_hash -> username.
            }
            self.storage.write_json(self.user_meta_path, meta_data)

        # Ensure default sysadmin user exists.
        meta_data = self.storage.read_json(self.user_meta_path)
        superuser_exists = False
        for user_hash, username in meta_data.get("users", {}).items():
            # Check if username is superuser (case-insensitive)
            if username.lower() == "superuser":
                superuser_exists = True
                break
        if not superuser_exists:
            # Retrieve valid roles from the roles meta file.
            valid_roles = self._get_valid_roles()
            admin_role_hash = None
            for role_hash, role_type in valid_roles.items():
                if role_type.lower() == "superadmin":
                    admin_role_hash = role_hash
                    break
            superuser_data = {
                "username": "superuser",
                "roles": [admin_role_hash] if admin_role_hash is not None else []
            }
            sysadmin_hash = self.create_user(superuser_data)
            logger.info(f"Default superuser user created with hash: {sysadmin_hash}")


    def _get_valid_roles(self) -> dict:
        """
        Retrieve valid roles from the roles meta file.
        Returns a dictionary mapping role_hash -> role type.
        """
        roles_meta_path = os.path.join(self.organization, self.super_name, "_roles.json")
        if not self.storage.exists(roles_meta_path):
            return {}
        meta_data = self.storage.read_json(roles_meta_path)
        return meta_data.get("roles", {})

    def create_user(self, data: dict) -> str:
        """
        Create a user.
        Data must include "username" and optionally "roles" (a list of role hashes).
        Validates that any assigned roles exist in the roles meta file.
        If a user with the same configuration (hash) already exists in _users.json,
        simply return its hash without modifying the meta-data.
        Returns the generated user hash.
        """
        if "username" not in data:
            raise ValueError("username is required")

        # Check if a user with the same username already exists.
        meta_data = self.storage.read_json(self.user_meta_path)
        for user_hash, existing_username in meta_data.get("users", {}).items():
            if existing_username.lower() == data["username"].lower():
                return user_hash

        roles = data.get("roles", [])
        valid_roles = self._get_valid_roles()
        for role_hash in roles:
            if role_hash not in valid_roles:
                raise ValueError(f"Role {role_hash} is not valid")

        # Build a base user data for stable hash calculation.
        base_user_data = {
            "username": data["username"],
            "roles": roles,
        }
        json_str = json.dumps(base_user_data, sort_keys=True)
        user_hash = hashlib.md5(json_str.encode()).hexdigest()
        # Build the final user data with timestamps.
        user_data = {
            "username": data["username"],
            "roles": roles,
            "created_ms": int(datetime.now().timestamp() * 1000),
            "modified_ms": int(datetime.now().timestamp() * 1000),
            "hash": user_hash
        }

        # Create a file named <user_hash>.json for the user's details.
        user_filename = user_hash + ".json"
        user_file_path = os.path.join(self.user_dir, user_filename)
        self.storage.write_json(user_file_path, user_data)

        # Update the meta file to store only the username for the given user hash.
        meta_data["users"][user_hash] = user_data["username"]
        meta_data["last_updated_ms"] = int(datetime.now().timestamp() * 1000)
        meta_data["version"] += 1
        self.storage.write_json(self.user_meta_path, meta_data)
        return user_hash

    def get_user(self, user_hash: str) -> dict:
        """
        Retrieve a user configuration based on the user hash.
        Constructs the filename as <user_hash>.json and returns the user JSON content.
        """
        meta_data = self.storage.read_json(self.user_meta_path)
        if user_hash not in meta_data.get("users", {}):
            raise ValueError(f"User {user_hash} does not exist")
        user_filename = user_hash + ".json"
        user_file_path = os.path.join(self.user_dir, user_filename)
        return self.storage.read_json(user_file_path)

    def get_user_hash_by_name(self, user_name: str) -> dict:
        """
        Retrieve a user configuration based on the user hash.
        Constructs the filename as <user_hash>.json and returns the user JSON content.
        """
        meta_data = self.storage.read_json(self.user_meta_path)
        user_hash = next(
            (key for key, value in meta_data.get("users", {}).items() if value == user_name),
            None  # Default if not found
        )

        if user_hash is None:
            raise ValueError(f"User {user_name} does not exist")

        user_data = self.get_user(user_hash)
        return user_data

    def modify_user(self, user_hash: str, data: dict) -> None:
        """
        Modify an existing user.
        Accepts fields like "username" and "roles". If roles are provided,
        validates that they exist in the roles meta file.
        """
        meta_data = self.storage.read_json(self.user_meta_path)
        if user_hash not in meta_data["users"]:
            raise ValueError(f"User {user_hash} does not exist")
        user_filename = user_hash + ".json"
        user_file_path = os.path.join(self.user_dir, user_filename)
        user_data = self.storage.read_json(user_file_path)

        if "roles" in data:
            roles = data["roles"]
            valid_roles = self._get_valid_roles()
            for role_hash in roles:
                if role_hash not in valid_roles:
                    raise ValueError(f"Role {role_hash} is not valid")
            user_data["roles"] = roles

        if "username" in data:
            user_data["username"] = data["username"]

        user_data["modified_ms"] = int(datetime.now().timestamp() * 1000)
        self.storage.write_json(user_file_path, user_data)

        # Update meta-data: if username has changed, update the value.
        meta_data["users"][user_hash] = user_data["username"]
        meta_data["last_updated_ms"] = int(datetime.now().timestamp() * 1000)
        meta_data["version"] += 1
        self.storage.write_json(self.user_meta_path, meta_data)

    def delete_user(self, user_hash: str) -> None:
        """
        Delete a user.
        Removes the user file and updates the meta-data file.
        The sysadmin user cannot be deleted.
        """
        meta_data = self.storage.read_json(self.user_meta_path)
        if user_hash not in meta_data["users"]:
            raise ValueError(f"User {user_hash} does not exist")

        # Check if the user is sysadmin; if so, prevent deletion.
        if meta_data["users"][user_hash].lower() == "sysadmin":
            raise ValueError("Sysadmin user cannot be deleted")

        # Proceed with deletion for non-sysadmin users.
        meta_data["users"].pop(user_hash)
        user_filename = user_hash + ".json"
        user_file_path = os.path.join(self.user_dir, user_filename)
        if self.storage.exists(user_file_path):
            os.remove(user_file_path)
        meta_data["last_updated_ms"] = int(datetime.now().timestamp() * 1000)
        meta_data["version"] += 1
        self.storage.write_json(self.user_meta_path, meta_data)

    def remove_role_from_users(self, role_hash: str) -> None:
        """
        Remove a role (by role_hash) from all users.
        Should be called when a role is deleted.
        """
        meta_data = self.storage.read_json(self.user_meta_path)
        for user_hash in meta_data["users"]:
            user_filename = user_hash + ".json"
            user_file_path = os.path.join(self.user_dir, user_filename)
            user_data = self.storage.read_json(user_file_path)
            if "roles" in user_data and role_hash in user_data["roles"]:
                user_data["roles"].remove(role_hash)
                user_data["modified_ms"] = int(datetime.now().timestamp() * 1000)
                self.storage.write_json(user_file_path, user_data)
        meta_data["last_updated_ms"] = int(datetime.now().timestamp() * 1000)
        meta_data["version"] += 1
        self.storage.write_json(self.user_meta_path, meta_data)
