# supertable/rbac/access_control.py

from dataclasses import dataclass
import hashlib
import hmac
import json
import re
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

from supertable.config.defaults import logger
from supertable.data_classes import TableDefinition
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.permissions import has_permission, Permission, RoleType
from supertable.rbac.filter_builder import FilterBuilder
from supertable.rbac.row_column_security import canonicalize_role_tables
from supertable.utils.sql_parser import SQLParser

if TYPE_CHECKING:
    from supertable.data_classes import RbacViewDef


@dataclass(frozen=True)
class RoleAccessContext:
    """One fully validated role policy for one SuperTable namespace."""

    role_info: dict
    role_type: RoleType
    tables: Dict[str, dict]
    fingerprint: str


_POLICY_FINGERPRINT_RE = re.compile(r"[0-9a-f]{64}")


def validate_policy_fingerprint(
    value: Optional[str],
    *,
    label: str = "policy fingerprint",
) -> Optional[str]:
    """Validate one canonical SHA-256 authorization-policy fingerprint."""
    if value is None:
        return None
    if not isinstance(value, str) or _POLICY_FINGERPRINT_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be 64 lowercase hexadecimal characters")
    return value


def _resolve_role(role_manager: RoleManager, role_name: str) -> dict:
    """Resolve a role_name to its role document.

    Raises ``PermissionError`` if the role does not exist or is disabled.
    """
    role_info = role_manager.get_role_by_name(role_name)
    if not role_info:
        logger.error(f"Role not found: {role_name}")
        raise PermissionError(f"Invalid or nonexistent role: {role_name}")
    # Check if role is disabled (enabled field missing = enabled for backward compat)
    enabled_val = role_info.get("enabled")
    if isinstance(enabled_val, bytes):
        enabled_val = enabled_val.decode("utf-8")
    if enabled_val is not None:
        enabled = (
            enabled_val
            if isinstance(enabled_val, bool)
            else str(enabled_val).strip().lower() in ("true", "1")
        )
    else:
        enabled = True
    if not enabled:
        logger.warning(f"Role '{role_name}' is disabled")
        raise PermissionError(f"Role '{role_name}' is disabled.")
    return role_info


def _normalize_tables(role_tables) -> dict:
    """Normalise the ``tables`` field from a role document.

    Handles both the **new** per-table dict format and the **legacy**
    list-of-table-names format that may still exist in Redis for roles
    created before the per-table RBAC redesign.

    Legacy ``["*"]``              → ``{"*": {"columns": ["*"], "filters": ["*"]}}``
    Legacy ``["t1", "t2"]``       → ``{"t1": {"columns": ["*"], "filters": ["*"]}, ...}``
    New    ``{"t1": {…}}``        → returned as-is
    Missing / ``None`` / ``{}``   → ``{}``
    """
    try:
        return canonicalize_role_tables(
            role_tables,
            default_if_empty=False,
            allow_legacy_list=True,
        )
    except (TypeError, ValueError) as exc:
        logger.error("Invalid persisted RBAC table policy: %s", exc)
        raise PermissionError("Invalid role policy") from exc


def _resolve_table_entry(role_tables: dict, table_name: str) -> Optional[dict]:
    """Look up a table's permission entry from the role definition.

    Returns the table-specific entry if it exists, otherwise the ``"*"``
    default entry.  Returns ``None`` if neither exists.
    """
    folded = str(table_name).casefold()
    match = None
    for configured_name, entry in role_tables.items():
        if configured_name == "*":
            continue
        if str(configured_name).casefold() == folded:
            if match is not None:
                # Canonicalisation already rejects this.  Keep the enforcement
                # boundary fail-closed if a direct caller bypasses it.
                raise PermissionError("Ambiguous table policy")
            match = entry
    if match is not None:
        return match
    return role_tables.get("*")


def _entry_allows_access(entry: Optional[dict]) -> bool:
    return isinstance(entry, dict) and entry.get("access", "allow") != "deny"


def _check_table_access(role_tables: dict, table_name: str, permission_label: str) -> None:
    """Raise ``PermissionError`` if the table is not covered by the role.

    ``permission_label`` is used in the error message (e.g. "WRITE", "META").
    """
    entry = _resolve_table_entry(role_tables, table_name)
    if not _entry_allows_access(entry):
        raise PermissionError(
            f"You don't have permission to {permission_label} table '{table_name}'."
        )


def _role_context(
    super_name: str,
    organization: str,
    role_name: str,
    permission: Permission,
    label: str,
) -> RoleAccessContext:
    role_manager = RoleManager(super_name=super_name, organization=organization)
    role_info = _resolve_role(role_manager, role_name)
    raw_type = role_info.get("role")
    try:
        role_type = RoleType(raw_type)
    except (TypeError, ValueError):
        logger.error("Role '%s' has invalid role type: %r", role_name, raw_type)
        raise PermissionError(f"You don't have permission to {label}.")
    if not has_permission(role_type, permission):
        raise PermissionError(f"You don't have permission to {label}.")
    tables = _normalize_tables(role_info.get("tables"))
    identity = {
        "role_id": role_info.get("role_id", ""),
        "role": role_type.value,
        "enabled": role_info.get("enabled", True),
        "tables": tables,
    }
    fingerprint = hashlib.sha256(json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")).hexdigest()
    return RoleAccessContext(role_info, role_type, tables, fingerprint)


def resolve_role_access_context(
    super_name: str,
    organization: str,
    role_name: str,
    permission: Permission = Permission.META,
    label: str = "META data",
) -> RoleAccessContext:
    """Public enforcement helper for metadata paths and cache identities."""
    return _role_context(super_name, organization, role_name, permission, label)


def resolve_table_policy(
    context: RoleAccessContext,
    table_name: str,
    permission_label: str = "access",
) -> dict:
    """Return the effective policy or deny the table.

    A role whose validated type is SUPERADMIN is the unconditional trusted
    control-plane bypass.
    ADMIN retains every operation permission, but an explicitly scoped ADMIN
    role now honours the same table/column denies as every other role.
    """
    if context.role_type is RoleType.SUPERADMIN:
        return {"columns": ["*"], "filters": ["*"]}
    entry = _resolve_table_entry(context.tables, table_name)
    if not _entry_allows_access(entry):
        raise PermissionError(
            f"You don't have permission to {permission_label} table '{table_name}'."
        )
    return entry


def visible_columns_for_policy(
    entry: dict,
    available_columns: Iterable[str],
) -> List[str]:
    """Resolve include-minus-exclude against actual schema spellings."""
    available = [str(name) for name in available_columns]
    folded_available: Dict[str, str] = {}
    for name in available:
        folded = name.casefold()
        if folded in folded_available:
            raise PermissionError("Schema contains case-colliding columns")
        folded_available[folded] = name
    allowed = entry.get("columns", ["*"])
    excluded = {str(name).casefold() for name in entry.get("exclude_columns", [])}
    if allowed == ["*"]:
        allowed_set = set(folded_available)
    else:
        allowed_set = {str(name).casefold() for name in allowed}
    return [
        name for name in available
        if name.casefold() in allowed_set and name.casefold() not in excluded
    ]


def _check_requested_columns(
    entry: dict,
    columns: Optional[Iterable[str]],
    table_name: str,
) -> None:
    if columns is None:
        return
    requested = {str(name).casefold() for name in columns}
    allowed = entry.get("columns", ["*"])
    excluded = {str(name).casefold() for name in entry.get("exclude_columns", [])}
    denied = requested & excluded
    if allowed != ["*"]:
        denied |= requested - {str(name).casefold() for name in allowed}
    if denied:
        raise PermissionError(
            f"You don't have permission to columns: {sorted(denied)} "
            f"in table '{table_name}'."
        )


def check_requested_columns(
    entry: dict,
    columns: Optional[Iterable[str]],
    table_name: str,
) -> None:
    """Enforce one resolved table policy against requested columns.

    Callers should obtain ``entry`` through :func:`resolve_table_policy`
    first.  This public boundary deliberately shares the same case-insensitive
    include-minus-exclude semantics used by SDK reads and mutations.
    """
    _check_requested_columns(entry, columns, table_name)


def _check_operation_access(
    super_name: str,
    organization: str,
    role_name: str,
    table_name: str,
    permission: Permission,
    label: str,
    columns: Optional[Iterable[str]] = None,
) -> Tuple[RoleAccessContext, dict]:
    """Shared enforcement for table-scoped operations.

    Checks:
      1. Role exists and has a role type.
      2. Role type has the required ``permission`` in the matrix.
      3. ``table_name`` is covered by the role's table definitions.

    Raises ``PermissionError`` on any failure.
    """
    context = _role_context(
        super_name, organization, role_name, permission, label,
    )
    entry = resolve_table_policy(context, table_name, label)
    _check_requested_columns(entry, columns, table_name)
    if permission in (Permission.WRITE, Permission.CREATE, Permission.CONTROL):
        # The current mutation pipeline publishes the incoming schema as the
        # newest logical schema and does not evaluate row predicates against
        # old/new rows.  Letting a column- or row-scoped role mutate the table
        # would therefore let it alter/delete data outside its visible slice,
        # even without sending an excluded column directly.  Until mutations
        # have a schema-preserving, predicate-certified path, scoped policies
        # are deliberately read-only.
        if (
            entry.get("columns", ["*"]) != ["*"]
            or bool(entry.get("exclude_columns"))
            or entry.get("filters", ["*"]) != ["*"]
        ):
            raise PermissionError(
                f"Scoped table policy for '{table_name}' is read-only for "
                f"{permission.name} operations."
            )
    return context, entry


def _check_readonly_guard(super_name: str, organization: str, label: str) -> None:
    """Block mutations on read-only SuperTables (snapshot clones, replicas, locked)."""
    from supertable.redis_catalog import RedisCatalog
    # Read-only/replica state controls whether a mutation is legal. Treating an
    # unavailable or corrupt root as writable can modify the wrong physical
    # table or diverge a replica, so lookup ambiguity must fail closed.
    root = RedisCatalog().get_root(organization, super_name)
    if root and (
        root.get("read_only") is True
        or root.get("clone_type") == "replica"
    ):
        clone_type = root.get("clone_type", "")
        if clone_type == "replica":
            reason = "live replica"
        elif clone_type == "readonly":
            reason = "read-only snapshot clone"
        elif root.get("cloned_from"):
            reason = "read-only clone"
        else:
            reason = "locked"
        raise PermissionError(
            f"This SuperTable is {reason}. Cannot {label}."
        )


def check_control_access(
    super_name: str,
    organization: str,
    role_name: str,
    table_name: str,
) -> None:
    """
    Check whether *role_name* is allowed to perform a CONTROL operation
    (e.g. drop table, truncate) on *table_name*.

    Raises ``PermissionError`` if the role lacks the necessary permission.
    """
    _check_readonly_guard(super_name, organization, "control this table")
    _check_operation_access(
        super_name, organization, role_name, table_name,
        Permission.CONTROL, "control this table",
    )


def check_write_access(
    super_name: str,
    organization: str,
    role_name: str,
    table_name: str,
    columns: Optional[Iterable[str]] = None,
) -> None:
    """
    Check whether *role_name* is allowed to WRITE to *table_name*.

    Raises ``PermissionError`` if the role lacks the necessary permission.
    """
    _check_readonly_guard(super_name, organization, "write to this table")
    _check_operation_access(
        super_name, organization, role_name, table_name,
        Permission.WRITE, "write to this table", columns=columns,
    )


def check_create_access(
    super_name: str,
    organization: str,
    role_name: str,
    table_name: str,
    columns: Optional[Iterable[str]] = None,
) -> None:
    """Check CREATE permission and the target table/column policy."""
    _check_readonly_guard(super_name, organization, "create this table")
    _check_operation_access(
        super_name, organization, role_name, table_name,
        Permission.CREATE, "create this table", columns=columns,
    )


def check_meta_access(
    super_name: str,
    organization: str,
    role_name: str,
    table_name: str,
) -> Tuple[RoleAccessContext, dict]:
    """
    Check whether *role_name* may read metadata for *table_name*.

    Raises ``PermissionError`` if the role lacks the necessary permission.
    """
    return _check_operation_access(
        super_name, organization, role_name, table_name,
        Permission.META, "META data",
    )


def restrict_read_access(
        super_name: str,
        organization: str,
        role_name: str,
        tables: List[TableDefinition],
        physical_tables: List[TableDefinition],
        aggregate_children: Optional[
            Dict[Tuple[str, str], Iterable[str]]
        ] = None,
        expected_role_policy_fingerprint: Optional[str] = None,
        policy_fingerprints_out: Optional[Dict[str, str]] = None,
) -> Dict[str, "RbacViewDef"]:
    """Authorize every physical table and build fail-closed RBAC views.

    Policies are resolved independently in each referenced SuperTable.  This
    is essential for qualified cross-super SQL: a wildcard role in the
    caller's default namespace must never authorize a table in another one.
    Exact table entries replace the wildcard entry, matching the persisted
    role contract; table and column comparisons are case-insensitive.
    """
    from supertable.data_classes import RbacViewDef

    expected_role_policy_fingerprint = validate_policy_fingerprint(
        expected_role_policy_fingerprint,
        label="expected_role_policy_fingerprint",
    )

    contexts: Dict[str, RoleAccessContext] = {}
    policies: Dict[Tuple[str, str], dict] = {}
    aggregates = {
        (str(key[0]).casefold(), str(key[1]).casefold()): tuple(
            str(child) for child in children
        )
        for key, children in (aggregate_children or {}).items()
    }

    # Preserve the historical empty-table superadmin check used by health and
    # bootstrap callers, while still validating the role document.
    namespaces = {str(pt.super_name) for pt in physical_tables}
    if not namespaces:
        namespaces.add(super_name)

    for namespace in namespaces:
        context = _role_context(
            namespace,
            organization,
            role_name,
            Permission.READ,
            "read the table",
        )
        contexts[namespace.casefold()] = context

    if policy_fingerprints_out is not None:
        if not isinstance(policy_fingerprints_out, dict):
            raise TypeError("policy_fingerprints_out must be a dict")
        policy_fingerprints_out.clear()
        policy_fingerprints_out.update({
            namespace: context.fingerprint
            for namespace, context in sorted(contexts.items())
        })

    if expected_role_policy_fingerprint is not None:
        expected_context = contexts.get(str(super_name).casefold())
        if expected_context is None or not hmac.compare_digest(
            expected_context.fingerprint,
            expected_role_policy_fingerprint,
        ):
            raise PermissionError("Role policy changed before query execution")

    for pt in physical_tables:
        context = contexts.get(str(pt.super_name).casefold())
        if context is None:
            raise PermissionError("No role policy for referenced SuperTable")

        physical_key = (
            str(pt.super_name).casefold(), str(pt.simple_name).casefold(),
        )
        child_names = aggregates.get(physical_key)
        target_names = child_names if child_names is not None else (pt.simple_name,)
        target_entries = []
        for target_name in target_names:
            entry = resolve_table_policy(context, target_name, "read")
            _check_requested_columns(entry, pt.columns or None, target_name)

            allowed = entry.get("columns", ["*"])
            excluded = {
                str(c).casefold() for c in entry.get("exclude_columns", [])
            }
            if allowed != ["*"] and not any(
                str(column).casefold() not in excluded for column in allowed
            ):
                raise PermissionError(
                    f"You don't have permission to read any columns in "
                    f"'{target_name}'."
                )
            target_entries.append(entry)

        if child_names is not None:
            # The executor currently materializes one union-by-name relation for
            # an aggregate. Apply the intersection of column grants and the union
            # of exclusions so no child's narrower policy is widened by another
            # child. Per-child row predicates cannot safely be represented after
            # the files have been collapsed; require them to be identical.
            if not target_entries:
                entry = {"columns": ["*"], "filters": ["*"]}
            else:
                constrained = [
                    {str(column).casefold() for column in item.get("columns", ["*"])}
                    for item in target_entries
                    if item.get("columns", ["*"]) != ["*"]
                ]
                if constrained:
                    allowed_folded = set.intersection(*constrained)
                    allowed = sorted(allowed_folded)
                else:
                    allowed = ["*"]
                exclusions = sorted({
                    str(column).casefold()
                    for item in target_entries
                    for column in item.get("exclude_columns", [])
                })
                filter_docs = {
                    json.dumps(
                        item.get("filters", ["*"]), sort_keys=True,
                        separators=(",", ":"), ensure_ascii=False,
                    )
                    for item in target_entries
                }
                if len(filter_docs) != 1:
                    raise PermissionError(
                        "Aggregate reads require the same row-filter policy "
                        "for every child table."
                    )
                filters = target_entries[0].get("filters", ["*"])
                entry = {
                    "columns": allowed,
                    "exclude_columns": exclusions,
                    "filters": filters,
                }
                _check_requested_columns(entry, pt.columns or None, pt.simple_name)
                if allowed != ["*"] and not (
                    {str(column).casefold() for column in allowed}
                    - set(exclusions)
                ):
                    raise PermissionError(
                        f"You don't have permission to read any columns in "
                        f"'{pt.simple_name}'."
                    )
        else:
            entry = target_entries[0]

        allowed = entry.get("columns", ["*"])
        excluded = {str(c).casefold() for c in entry.get("exclude_columns", [])}
        if allowed != ["*"] and not any(
            str(column).casefold() not in excluded for column in allowed
        ):
            raise PermissionError(
                f"You don't have permission to read any columns in '{pt.simple_name}'."
            )

        policies[physical_key] = entry

    rbac_views: Dict[str, RbacViewDef] = {}
    for td in tables:
        key = (str(td.super_name).casefold(), str(td.simple_name).casefold())
        entry = policies.get(key)
        if entry is None:
            # CTE aliases are not physical relations and are secured through
            # their transitive source views.
            continue
        context = contexts[key[0]]
        if context.role_type is RoleType.SUPERADMIN:
            continue

        allowed_columns = list(entry.get("columns", ["*"]))
        excluded_columns = list(entry.get("exclude_columns", []))
        filters = entry.get("filters", ["*"])
        where_clause = ""
        if filters != ["*"]:
            try:
                generated = FilterBuilder(
                    table_name="__PLACEHOLDER__",
                    columns=["*"],
                    role_info={"filters": filters},
                ).filter_query
            except (KeyError, TypeError, ValueError) as exc:
                raise PermissionError("Invalid role row-filter policy") from exc
            where_idx = generated.upper().find("WHERE ")
            if where_idx < 0:
                raise PermissionError("Invalid role row-filter policy")
            where_clause = generated[where_idx + 6:]

        if (
            allowed_columns != ["*"]
            or excluded_columns
            or where_clause
        ):
            rbac_views[td.alias] = RbacViewDef(
                allowed_columns=allowed_columns,
                where_clause=where_clause,
                excluded_columns=excluded_columns,
                filter_spec=filters,
            )

    return rbac_views
