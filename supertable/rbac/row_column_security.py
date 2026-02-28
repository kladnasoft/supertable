# supertable/rbac/row_column_security.py

import json
import hashlib
from typing import List, Optional, Dict, Union

from supertable.rbac.permissions import RoleType


class RowColumnSecurity:
    """
    Value object that validates and normalises role permission data.

    * ``role``    – one of the ``RoleType`` enum values.
    * ``tables``  – list of table names, or ``["*"]`` for all.
    * ``columns`` – list of column names, or ``["*"]`` for all.
    * ``filters`` – filter definition (dict/list), or ``["*"]`` for none.
    * ``content_hash`` – deterministic hash of the *content* above.
      Used for change-detection / logging, **not** as the identity.
      The stable identity is ``role_id`` (UUID), assigned by RoleManager.
    """

    def __init__(
            self,
            role: str,
            tables: List[str],
            columns: Optional[List[str]] = None,
            filters: Optional[Union[List, Dict]] = None,
            role_name: Optional[str] = None,
    ):
        # Convert the string role to RoleType from the permissions module.
        self.role = RoleType(role)
        self.tables = tables
        self.columns = columns
        self.filters = filters
        self.role_name = role_name
        self.content_hash: Optional[str] = None

    def sort_all(self):
        """Ensure tables and columns are unique and sorted for consistency."""
        self.tables = sorted(set(self.tables))
        if self.columns and self.columns != ["*"]:
            self.columns = sorted(set(self.columns))

    def to_json(self) -> dict:
        """Return a dict representation of the role data."""
        return {
            "role": self.role.value,
            "tables": self.tables,
            "columns": self.columns,
            "filters": self.filters,
        }

    def create_content_hash(self) -> None:
        """Create an MD5 hash based on the JSON representation of the role content."""
        json_str = json.dumps(self.to_json(), sort_keys=True)
        self.content_hash = hashlib.md5(json_str.encode()).hexdigest()

    def prepare(self) -> None:
        """Validate role parameters, apply defaults, and compute content hash."""
        if not self.tables:
            self.tables = ["*"]
        if not self.columns:
            self.columns = ["*"]
        if not self.filters:
            self.filters = ["*"]
        self.sort_all()
        self.create_content_hash()

    # Backward-compatible alias ------------------------------------------------
    # Old code may reference ``.hash``; redirect to ``content_hash``.
    @property
    def hash(self) -> Optional[str]:
        return self.content_hash

    def create_hash(self) -> None:
        """Deprecated: use ``create_content_hash`` instead."""
        self.create_content_hash()