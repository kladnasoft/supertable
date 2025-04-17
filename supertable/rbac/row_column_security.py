import json
import hashlib
from typing import List, Optional, Dict, Union

from supertable.rbac.permissions import RoleType


class RowColumnSecurity:
    def __init__(
            self,
            role: str,
            tables: List[str],
            columns: Optional[List[str]] = None,
            filters: Optional[Dict[str, Union[List, Dict]]] = None,
    ):
        # Convert the string role to RoleType from your permissions module.
        self.role = RoleType(role)
        self.tables = tables
        self.columns = columns
        self.filters = filters
        self.hash: Optional[str] = None

    def sort_all(self):
        """Ensure tables and columns are unique and sorted for consistency."""
        self.tables = sorted(set(self.tables))
        if self.columns:
            self.columns = sorted(set(self.columns))

    def to_json(self) -> dict:
        """Return a dict representation of the role data."""
        return {
            "role": self.role.value,
            "tables": self.tables,
            "columns": self.columns,
            "filters": self.filters,
        }

    def create_hash(self):
        """Create an MD5 hash based on the JSON representation of the role."""
        json_str = json.dumps(self.to_json(), sort_keys=True)
        self.hash = hashlib.md5(json_str.encode()).hexdigest()

    def prepare(self):
        """Validate role parameters and create a unique hash."""
        # For tables, an empty list means "all tables", so we convert it to a wildcard.
        if not self.tables:
            self.tables = ["*"]
        if not self.columns:
            self.columns = ["*"]
        if not self.filters:
            self.filters = ["*"]
        self.sort_all()
        self.create_hash()
