"""Validation and canonicalisation for persisted RBAC role policies.

The role document is a security boundary: callers must never have to guess
whether a malformed value means "allow" or "deny".  This module therefore
owns the one canonical representation used by the public RoleManager, the
catalog write boundary, and read-time access checks.
"""

from __future__ import annotations

import hashlib
import json
import math
from typing import Dict, Optional

from supertable.rbac.permissions import RoleType


# Public, deterministic input budgets.  They limit both persisted policy size
# and the work performed before that final byte-size check.
MAX_ROLE_TABLES = 1_024
MAX_ROLE_COLUMNS_PER_TABLE = 4_096
MAX_ROLE_COLUMN_RULES = 16_384
MAX_ROLE_IDENTIFIER_BYTES = 255
MAX_ROLE_FILTER_DEPTH = 32
MAX_ROLE_FILTER_NODES = 16_384
MAX_ROLE_DOCUMENT_BYTES = 1 * 1024 * 1024
MAX_ROLE_SCALAR_BYTES = 64 * 1024

_TABLE_ENTRY_FIELDS = frozenset({
    "access", "columns", "exclude_columns", "filters",
})
_ACCESS_VALUES = frozenset({"allow", "deny"})


def _validate_identifier(value: object, *, label: str, wildcard: bool) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a string")
    if not value:
        raise ValueError(f"{label} must not be empty")
    if value == "*" and not wildcard:
        raise ValueError(f"{label} cannot be '*'")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{label} must be valid UTF-8") from exc
    if len(encoded) > MAX_ROLE_IDENTIFIER_BYTES:
        raise ValueError(
            f"{label} exceeds the {MAX_ROLE_IDENTIFIER_BYTES}-byte limit"
        )
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"{label} contains a control character")
    return value


def _canonicalize_name_list(
    raw: object,
    *,
    label: str,
    allow_wildcard: bool,
) -> list[str]:
    if not isinstance(raw, list):
        raise ValueError(f"{label} must be a list of column names")
    if len(raw) > MAX_ROLE_COLUMNS_PER_TABLE:
        raise ValueError(
            f"{label} exceeds the {MAX_ROLE_COLUMNS_PER_TABLE}-column limit"
        )

    values: list[str] = []
    seen: dict[str, str] = {}
    for index, item in enumerate(raw):
        name = _validate_identifier(
            item, label=f"{label}[{index}]", wildcard=allow_wildcard,
        )
        folded = name.casefold()
        previous = seen.get(folded)
        if previous is not None:
            if previous != name:
                raise ValueError(
                    f"{label} contains case-colliding names "
                    f"{previous!r} and {name!r}"
                )
            # Preserve the legacy contract: exact duplicates are canonicalised
            # away instead of making an otherwise valid old role unreadable.
            continue
        seen[folded] = name
        values.append(name)

    if "*" in values and values != ["*"]:
        raise ValueError(f"{label} wildcard '*' must be the only entry")
    return sorted(values, key=lambda value: (value.casefold(), value))


def _canonicalize_filter_json(
    raw: object,
    *,
    remaining: Optional[list[int]] = None,
) -> object:
    """Copy a filter tree while enforcing bounded, JSON-safe structure.

    FilterBuilder remains the owner of the predicate grammar.  This boundary
    deliberately accepts the historical dict/list shapes, but rejects Python
    objects, non-string keys, non-finite numbers, and adversarially deep or
    wide documents before they reach Redis or recursive query construction.
    """

    if remaining is None:
        remaining = [MAX_ROLE_FILTER_NODES]

    def visit(value: object, depth: int) -> object:
        remaining[0] -= 1
        if remaining[0] < 0:
            raise ValueError(
                f"role filters exceed the {MAX_ROLE_FILTER_NODES}-node limit"
            )
        if depth > MAX_ROLE_FILTER_DEPTH:
            raise ValueError(
                f"role filters exceed the depth limit of {MAX_ROLE_FILTER_DEPTH}"
            )

        if isinstance(value, dict):
            result: dict[str, object] = {}
            for key, child in value.items():
                if not isinstance(key, str):
                    raise ValueError("role filter object keys must be strings")
                if len(key.encode("utf-8")) > MAX_ROLE_IDENTIFIER_BYTES:
                    raise ValueError("role filter key exceeds the identifier limit")
                result[key] = visit(child, depth + 1)
            return result
        if isinstance(value, list):
            return [visit(child, depth + 1) for child in value]
        if value is None or isinstance(value, bool):
            return value
        if isinstance(value, int):
            # Avoid pathological integers whose decimal conversion itself is
            # an unbounded operation on supported Python versions.
            if value.bit_length() > MAX_ROLE_SCALAR_BYTES * 3:
                raise ValueError("role filter integer exceeds the scalar limit")
            return value
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError("role filter numbers must be finite")
            return value
        if isinstance(value, str):
            if len(value.encode("utf-8")) > MAX_ROLE_SCALAR_BYTES:
                raise ValueError(
                    f"role filter string exceeds the {MAX_ROLE_SCALAR_BYTES}-byte limit"
                )
            return value
        raise ValueError(
            f"role filters must contain only JSON values, got {type(value).__name__}"
        )

    return visit(raw, 0)


def _legacy_table_list(raw: list) -> Dict[str, dict]:
    """Convert the pre-per-table list representation without granting more."""
    if len(raw) > MAX_ROLE_TABLES:
        raise ValueError(
            f"role tables exceed the {MAX_ROLE_TABLES}-entry limit"
        )
    converted: Dict[str, dict] = {}
    seen: dict[str, str] = {}
    for index, item in enumerate(raw):
        name = _validate_identifier(
            item, label=f"tables[{index}]", wildcard=True,
        )
        folded = name.casefold()
        previous = seen.get(folded)
        if previous is not None and previous != name:
            raise ValueError(
                f"tables contains case-colliding names {previous!r} and {name!r}"
            )
        if previous is not None:
            continue
        seen[folded] = name
        converted[name] = {"columns": ["*"], "filters": ["*"]}
    return converted


def canonicalize_role_tables(
    role_tables: object,
    *,
    default_if_empty: bool = False,
    allow_legacy_list: bool = True,
) -> Dict[str, dict]:
    """Validate and return the canonical per-table role policy.

    ``default_if_empty`` is intentionally false.  Access checks reading a
    missing/corrupt persisted policy therefore see no table grants.  The
    historical public creation API opts in to its old empty-means-wildcard
    behaviour through :meth:`RowColumnSecurity.prepare`.

    Exact table entries override the ``"*"`` entry at enforcement time.
    ``access: deny`` is an absolute table denial.  ``exclude_columns`` is a
    case-insensitive deny overlay on the legacy ``columns`` inclusion list.
    """
    if role_tables is None:
        if default_if_empty:
            role_tables = {}
        else:
            raise ValueError("role tables are missing")
    if isinstance(role_tables, list):
        if not allow_legacy_list:
            raise ValueError("legacy role table lists are not accepted here")
        role_tables = _legacy_table_list(role_tables)
    if not isinstance(role_tables, dict):
        raise ValueError("role tables must be an object")
    if not role_tables and default_if_empty:
        role_tables = {"*": {"columns": ["*"], "filters": ["*"]}}
    if len(role_tables) > MAX_ROLE_TABLES:
        raise ValueError(
            f"role tables exceed the {MAX_ROLE_TABLES}-entry limit"
        )

    table_items: list[tuple[str, dict]] = []
    seen_tables: dict[str, str] = {}
    total_column_rules = 0
    filter_node_budget = [MAX_ROLE_FILTER_NODES]

    for raw_name, raw_entry in role_tables.items():
        table_name = _validate_identifier(
            raw_name, label="table name", wildcard=True,
        )
        folded_table = table_name.casefold()
        previous_table = seen_tables.get(folded_table)
        if previous_table is not None and previous_table != table_name:
            raise ValueError(
                "role tables contains case-colliding names "
                f"{previous_table!r} and {table_name!r}"
            )
        seen_tables[folded_table] = table_name

        if not isinstance(raw_entry, dict):
            raise ValueError(f"table policy for {table_name!r} must be an object")
        unknown = set(raw_entry) - _TABLE_ENTRY_FIELDS
        if unknown:
            raise ValueError(
                f"table policy for {table_name!r} has unknown fields: "
                f"{sorted(str(field) for field in unknown)}"
            )

        access = raw_entry.get("access", "allow")
        if not isinstance(access, str) or access not in _ACCESS_VALUES:
            raise ValueError(
                f"table policy access for {table_name!r} must be 'allow' or 'deny'"
            )

        if access == "deny":
            contradictory = set(raw_entry) - {"access"}
            if contradictory:
                raise ValueError(
                    f"denied table policy {table_name!r} cannot also define: "
                    f"{sorted(contradictory)}"
                )
            table_items.append((table_name, {"access": "deny"}))
            continue

        columns = _canonicalize_name_list(
            raw_entry.get("columns", ["*"]),
            label=f"tables[{table_name!r}].columns",
            allow_wildcard=True,
        )
        excluded = _canonicalize_name_list(
            raw_entry.get("exclude_columns", []),
            label=f"tables[{table_name!r}].exclude_columns",
            allow_wildcard=False,
        )
        total_column_rules += len(columns) + len(excluded)
        if total_column_rules > MAX_ROLE_COLUMN_RULES:
            raise ValueError(
                f"role exceeds the {MAX_ROLE_COLUMN_RULES}-column-rule limit"
            )

        filters = _canonicalize_filter_json(
            raw_entry.get("filters", ["*"]), remaining=filter_node_budget,
        )
        if isinstance(filters, list) and "*" in filters and filters != ["*"]:
            raise ValueError(
                f"tables[{table_name!r}].filters wildcard '*' must be the only entry"
            )
        # Validate the complete predicate grammar at the persistence boundary,
        # not for the first time on a user query.  A malformed stored filter
        # must never turn an otherwise valid role into a runtime 500/DoS.
        from supertable.rbac.filter_builder import FilterBuilder
        try:
            FilterBuilder(
                "__rbac_policy_validation__",
                ["*"],
                {"filters": filters},
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                f"invalid filters for table policy {table_name!r}: {exc}"
            ) from exc

        entry: dict[str, object] = {
            "columns": columns,
            "filters": filters,
        }
        # Omit semantic defaults so legacy and new spellings share one hash.
        # A non-empty deny overlay and an absolute table denial remain explicit.
        if excluded:
            entry["exclude_columns"] = excluded
        table_items.append((table_name, entry))

    table_items.sort(key=lambda item: (item[0].casefold(), item[0]))
    canonical = dict(table_items)
    # Measure the policy alone here.  RowColumnSecurity performs the complete
    # role-document measurement including the role type.
    encoded = json.dumps(
        canonical, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    ).encode("utf-8")
    if len(encoded) > MAX_ROLE_DOCUMENT_BYTES:
        raise ValueError(
            f"role tables exceed the {MAX_ROLE_DOCUMENT_BYTES}-byte limit"
        )
    return canonical


class RowColumnSecurity:
    """Validated, canonical role permission content.

    ``tables`` maps table names to entries with the legacy ``columns`` and
    ``filters`` fields plus two opt-in exclusion fields:

    * ``access: "deny"`` denies every operation on that table.
    * ``exclude_columns: [...]`` removes named columns from the inclusion set.

    A specific table entry takes precedence over ``"*"``.  Policy matching is
    case-insensitive; case-colliding definitions are rejected at write time.
    """

    def __init__(
        self,
        role: str,
        tables: Optional[Dict[str, dict]] = None,
        role_name: Optional[str] = None,
    ):
        self.role = RoleType(role)
        self.tables: object = tables
        self.role_name = role_name
        self.content_hash: Optional[str] = None

    def sort_all(self) -> None:
        """Canonicalise tables and column lists (backward-compatible API)."""
        self.tables = canonicalize_role_tables(
            self.tables, default_if_empty=True, allow_legacy_list=False,
        )

    def to_json(self) -> dict:
        return {
            "role": self.role.value,
            "tables": self.tables,
        }

    def create_content_hash(self) -> None:
        document = self.to_json()
        measured = json.dumps(
            document, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False,
        ).encode("utf-8")
        if len(measured) > MAX_ROLE_DOCUMENT_BYTES:
            raise ValueError(
                f"role document exceeds the {MAX_ROLE_DOCUMENT_BYTES}-byte limit"
            )
        # Retain the historical serialisation byte-for-byte for legacy role
        # documents so upgrading does not manufacture a content change.
        json_bytes = json.dumps(document, sort_keys=True).encode("utf-8")
        # MD5 remains the persisted compatibility format.  It is a change
        # detector, never an authentication or authorization primitive.
        self.content_hash = hashlib.md5(json_bytes).hexdigest()

    def prepare(self) -> None:
        """Validate, canonicalise, apply creation defaults, and hash."""
        self.sort_all()
        self.create_content_hash()

    @property
    def hash(self) -> Optional[str]:
        """Deprecated alias for :attr:`content_hash`."""
        return self.content_hash

    def create_hash(self) -> None:
        """Deprecated: use :meth:`create_content_hash`."""
        self.create_content_hash()
