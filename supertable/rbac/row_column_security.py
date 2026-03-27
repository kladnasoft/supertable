# supertable/rbac/row_column_security.py

import json
import hashlib
from typing import Dict, Optional

from supertable.rbac.permissions import RoleType


class RowColumnSecurity:
    """
    Value object that validates and normalises role permission data.

    * ``role``    – one of the ``RoleType`` enum values.
    * ``tables``  – dict mapping table names to per-table definitions.
      Each entry is ``{"columns": [...], "filters": [...]}``.
      A ``"*"`` key acts as the default for tables not explicitly listed.
      Example::

          {
              "*": {"columns": ["*"], "filters": ["*"]},
              "orders": {
                  "columns": ["order_id", "amount", "status"],
                  "filters": [{"status": {"operation": "=",
                                          "type": "value",
                                          "value": "completed"}}]
              },
              "customers": {
                  "columns": ["customer_id", "name", "region"],
                  "filters": ["*"]
              }
          }

    * ``content_hash`` – deterministic hash of the *content* above.
      Used for change-detection / logging, **not** as the identity.
      The stable identity is ``role_id`` (UUID), assigned by RoleManager.
    """

    def __init__(
            self,
            role: str,
            tables: Optional[Dict[str, dict]] = None,
            role_name: Optional[str] = None,
    ):
        # Convert the string role to RoleType from the permissions module.
        self.role = RoleType(role)
        self.tables: Dict[str, dict] = tables or {}
        self.role_name = role_name
        self.content_hash: Optional[str] = None

    def sort_all(self) -> None:
        """Ensure per-table column lists are unique and sorted for consistency."""
        for table_name, table_def in self.tables.items():
            cols = table_def.get("columns", ["*"])
            if cols and cols != ["*"]:
                table_def["columns"] = sorted(set(cols))

    def to_json(self) -> dict:
        """Return a dict representation of the role data."""
        return {
            "role": self.role.value,
            "tables": self.tables,
        }

    def create_content_hash(self) -> None:
        """Create an MD5 hash based on the JSON representation of the role content."""
        json_str = json.dumps(self.to_json(), sort_keys=True)
        self.content_hash = hashlib.md5(json_str.encode()).hexdigest()

    def prepare(self) -> None:
        """Validate role parameters, apply defaults, and compute content hash."""
        if not self.tables:
            self.tables = {"*": {"columns": ["*"], "filters": ["*"]}}

        for table_name, table_def in self.tables.items():
            if "columns" not in table_def:
                table_def["columns"] = ["*"]
            if "filters" not in table_def:
                table_def["filters"] = ["*"]

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
