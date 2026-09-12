# supertable/rbac/row_column_security.py

import json
import hashlib
from typing import Dict, List, Optional

from supertable.rbac.permissions import RoleType

WILDCARD = "*"


def normalize_allowed_columns(columns) -> List[str]:
    """Return the fail-closed reading of a per-table ``columns`` list.

    ``None`` / key absent        → ``["*"]``   (the documented default)
    ``["*"]``                    → ``["*"]``   (the unrestricted sentinel)
    ``["id", "*"]``              → ``["id"]``  (the ``"*"`` never widens)
    ``[]`` / only blanks         → ``[]``      (empty means none → deny)

    The mixed case is S7: ``allowed_columns=["id", "*"]`` reached the view
    builder verbatim and emitted ``SELECT id, *``, handing back every column
    the mask was supposed to hide.  ``"*"`` is not a real column name, so the
    only safe reading of a list that also names columns is *those columns*.
    """
    if columns is None:
        return [WILDCARD]
    if isinstance(columns, str):
        columns = [columns]
    if not isinstance(columns, (list, tuple, set)):
        raise ValueError(f"Invalid columns definition in RBAC role: {columns!r}")

    cols = [str(c).strip() for c in columns]
    if not cols:
        return []
    named = [c for c in cols if c and c != WILDCARD]
    if named:
        return named
    if all(c == WILDCARD for c in cols):
        return [WILDCARD]
    return []


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
        """Validate role parameters, apply defaults, and compute content hash.

        An **empty** ``tables`` mapping means *no grant at all* and is stored
        verbatim.  It used to be rewritten to ``{"*": {"columns": ["*"],
        "filters": ["*"]}}`` — so revoking a role's last table grant promoted
        it to every column of every table (C4).  "Empty means none" is already
        the rule one level down (an empty ``columns`` list correctly denies);
        the table level now agrees.  A caller that genuinely wants everything
        must say so with an explicit ``{"*": ...}`` entry.
        """
        for table_name, table_def in self.tables.items():
            if "columns" not in table_def:
                table_def["columns"] = ["*"]
            if "filters" not in table_def:
                table_def["filters"] = ["*"]
            self._reject_mixed_wildcard(table_name, table_def["columns"])

        self.sort_all()
        self.create_content_hash()

    @staticmethod
    def _reject_mixed_wildcard(table_name: str, columns) -> None:
        """Refuse a ``columns`` list that mixes ``"*"`` with named columns.

        ``["id", "*"]`` is ambiguous: read as "everything" it silently defeats
        the mask the operator wrote (S7).  Rather than guess, reject it at
        write time so the intent has to be stated.  Documents already in Redis
        are narrowed at read time by :func:`normalize_allowed_columns`.
        """
        if not isinstance(columns, (list, tuple, set)):
            return
        cols = [str(c).strip() for c in columns]
        named = [c for c in cols if c and c != WILDCARD]
        if named and any(c == WILDCARD for c in cols):
            raise ValueError(
                f"Invalid columns for table {table_name!r}: {list(columns)!r}. "
                f"'*' cannot be combined with named columns — use ['*'] for "
                f"unrestricted access or list the columns explicitly."
            )

    # Backward-compatible alias ------------------------------------------------
    # Old code may reference ``.hash``; redirect to ``content_hash``.
    @property
    def hash(self) -> Optional[str]:
        return self.content_hash

    def create_hash(self) -> None:
        """Deprecated: use ``create_content_hash`` instead."""
        self.create_content_hash()
