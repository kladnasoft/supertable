import os

from datetime import datetime
from supertable.rbac.row_column_security import RowColumnSecurity
from supertable.storage.storage_factory import get_storage
from supertable.locking import Locking
from supertable.config.defaults import logger

class RoleManager:
    def __init__(self, super_name: str, organization: str):
        """
        super_name: Base directory name where roles will be stored.
        The storage backend is chosen via get_storage().
        """
        self.module = "rbac"
        self.identity = "roles"
        self.super_name = super_name
        self.organization = organization

        # Choose the storage backend via a factory.
        self.storage = get_storage()

        # Create a directory for role files and a meta-data file to track roles.
        self.role_dir = os.path.join(self.organization, self.super_name, self.module, self.identity)
        self.role_meta_path = os.path.join(self.organization, self.super_name, "_roles.json")
        logger.debug(f"role_dir: {self.role_dir}")
        logger.debug(f"role_meta: {self.role_meta_path}")
        self.locking = Locking(identity=self.super_name, working_dir=self.role_dir)
        self.init_role_storage()

    def init_role_storage(self) -> None:
        # Create the directory for role files.
        self.storage.makedirs(self.role_dir)

        meta_data = None
        if self.storage.exists(self.role_meta_path):
            try:
                meta_data = self.storage.read_json(self.role_meta_path)
            except Exception as e:
                logger.exception(f"Warning: Meta file invalid, reinitializing: {e}")

        if meta_data is None:
            # Initialize meta-data if it doesn't exist or is invalid.
            meta_data = {
                "version": 0,
                "last_updated_ms": int(datetime.now().timestamp() * 1000),
                "roles": {}  # Mapping: role_hash -> role type
            }
            self.storage.write_json(self.role_meta_path, meta_data)
            # Create the default admin role which has all tables, rows, and columns.
            # For admin, an empty "tables" list is interpreted (in RowColumnSecurity.prepare())
            # as "all tables" (for rows/columns as needed).
            sysadmin_data = {
                "role": "superadmin",
                "tables": [],
                "columns": [],
                "filters": []
            }
            sysadmin_hash = self.create_role(sysadmin_data)
            logger.info(f"Default sysadmin role created with hash: {sysadmin_hash}")

    def create_role(self, data: dict) -> str:
        """
        Create a role from provided data and store it.
        If a role with the same configuration (hash) already exists,
        simply return its hash without modifying the meta-data.
        :param data: A dictionary containing role details.
        :return: The generated role hash.
        """
        # Create a role (with row/column security) from the provided data.
        role_data = RowColumnSecurity(**data)
        role_data.prepare()

        # Read meta-data file to check if this role already exists.
        meta_data = self.storage.read_json(self.role_meta_path)
        if "roles" in meta_data and role_data.hash in meta_data["roles"]:
            # Role already exists, so simply return its hash.
            return role_data.hash

        # Otherwise, prepare the role content.
        role_content = role_data.to_json()
        role_content["hash"] = role_data.hash

        # Name the role file as <role_hash>.json.
        role_filename = role_data.hash + ".json"
        role_file_path = os.path.join(self.role_dir, role_filename)

        # Write the role content to the file.
        self.storage.write_json(role_file_path, role_content)

        # Update the meta-data file: store only the mapping hash -> role type.
        if "roles" not in meta_data:
            meta_data["roles"] = {}
        meta_data["roles"][role_data.hash] = role_data.role.value
        meta_data["last_updated_ms"] = int(datetime.now().timestamp() * 1000)
        meta_data["version"] += 1
        self.storage.write_json(self.role_meta_path, meta_data)

        return role_data.hash

    def delete_role(self, role_hash: str) -> bool:
        """
        Delete a role by its role hash.
        This method removes the role entry from the meta-data file and deletes the corresponding role file.
        :param role_hash: The hash of the role to delete.
        :return: True if deletion was successful, False otherwise.
        """
        meta_data = self.storage.read_json(self.role_meta_path)
        if "roles" not in meta_data or role_hash not in meta_data["roles"]:
            return False

        # Remove the role entry from the meta-data.
        del meta_data["roles"][role_hash]
        meta_data["last_updated_ms"] = int(datetime.now().timestamp() * 1000)
        meta_data["version"] += 1
        self.storage.write_json(self.role_meta_path, meta_data)

        # Delete the corresponding role file.
        role_file_path = os.path.join(self.role_dir, role_hash + ".json")
        if self.storage.exists(role_file_path):
            os.remove(role_file_path)
        return True

    def get_role(self, role_hash: str) -> dict:
        """
        Retrieve a role configuration based on the role hash.
        Returns the role content (including tables, columns, filters, and hash)
        by reading the corresponding role file.
        """
        role_file_path = os.path.join(self.role_dir, role_hash + ".json")
        if self.storage.exists(role_file_path):
            return self.storage.read_json(role_file_path)
        return {}

    def list_roles(self) -> list:
        """
        List all stored roles.
        Returns a list of role configurations (each including tables, columns, filters, and hash)
        by reading the meta file and the corresponding role files.
        """
        meta_data = self.storage.read_json(self.role_meta_path)
        roles_list = []
        if "roles" in meta_data:
            for role_hash in meta_data["roles"]:
                role_file_path = os.path.join(self.role_dir, role_hash + ".json")
                if self.storage.exists(role_file_path):
                    role_conf = self.storage.read_json(role_file_path)
                    roles_list.append(role_conf)
        return roles_list
