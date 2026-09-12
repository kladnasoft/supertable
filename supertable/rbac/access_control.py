# supertable/rbac/access_control.py

from typing import Any, Dict, List, Tuple

import redis as _redis

from supertable.config.defaults import logger
from supertable.data_classes import TableDefinition
# Imported at module scope, not per call: ``_check_readonly_guard`` runs on
# every write, and both of these were function-local — a needless sys.modules
# lookup and frame setup on the hot path. No cycle: ``redis_catalog`` imports
# ``rbac.permissions`` (a leaf) and never imports this module.
from supertable.redis_catalog import RedisCatalog
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.permissions import has_permission, Permission, RoleType
from supertable.rbac.filter_builder import FilterBuilder
from supertable.rbac.row_column_security import normalize_allowed_columns
from supertable.utils.sql_parser import SQLParser


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
    if isinstance(enabled_val, str) and enabled_val.lower() in ("false", "0"):
        logger.warning(f"Role '{role_name}' is disabled")
        raise PermissionError(f"Role '{role_name}' is disabled.")
    if isinstance(enabled_val, bool) and not enabled_val:
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
    if isinstance(role_tables, dict):
        return role_tables
    if isinstance(role_tables, list):
        return {
            t: {"columns": ["*"], "filters": ["*"]}
            for t in role_tables
        }
    return {}


def _resolve_table_entry(role_tables: dict, table_name: str) -> dict:
    """Look up a table's permission entry from the role definition.

    Returns the table-specific entry if it exists, otherwise the ``"*"``
    default entry.  Returns ``None`` if neither exists.
    """
    return role_tables.get(table_name) or role_tables.get("*")


def _check_table_access(role_tables: dict, table_name: str, permission_label: str) -> None:
    """Raise ``PermissionError`` if the table is not covered by the role.

    ``permission_label`` is used in the error message (e.g. "WRITE", "META").
    """
    entry = _resolve_table_entry(role_tables, table_name)
    if entry is None:
        raise PermissionError(
            f"You don't have permission to {permission_label} table '{table_name}'."
        )


def _check_operation_access(
    super_name: str,
    organization: str,
    role_name: str,
    table_name: str,
    permission: Permission,
    label: str,
) -> None:
    """Shared enforcement for table-scoped operations.

    Checks:
      1. Role exists and has a role type.
      2. Role type has the required ``permission`` in the matrix.
      3. ``table_name`` is covered by the role's table definitions.

    Raises ``PermissionError`` on any failure.
    """
    role_info = _check_scope_access(
        super_name, organization, role_name, permission, label,
    )
    role_tables = _normalize_tables(role_info.get("tables", {}))
    _check_table_access(role_tables, table_name, label)


def _check_scope_access(
    super_name: str,
    organization: str,
    role_name: str,
    permission: Permission,
    label: str,
) -> dict:
    """Resolve a role and require *permission*, without any table scoping.

    The SuperTable-wide half of :func:`_check_operation_access`. Split out
    because some operations are not table-scoped at all — administering roles
    and users is a property of the SuperTable, and passing a table name for it
    would mean inventing one and then checking the role's grants against a
    table that does not exist.

    Returns the role document so a table-scoped caller can go on to check
    grants against it.
    """
    role_manager = RoleManager(super_name=super_name, organization=organization)
    role_info = _resolve_role(role_manager, role_name)

    role_type_str = role_info.get("role")
    if not role_type_str:
        logger.error(f"Role '{role_name}' has no role type")
        raise PermissionError(f"You don't have permission to {label}.")

    try:
        role_type = RoleType(role_type_str)
    except ValueError:
        logger.error(f"Role '{role_name}' has invalid role type: {role_type_str}")
        raise PermissionError(f"You don't have permission to {label}.")

    if not has_permission(role_type, permission):
        logger.error(f"Role '{role_name}' does not have {permission.name} permission.")
        raise PermissionError(f"You don't have permission to {label}.")

    return role_info


def _check_readonly_guard(super_name: str, organization: str, label: str) -> None:
    """Block mutations on read-only SuperTables (snapshot clones, replicas, locked).

    Failure handling is split on purpose. A Redis outage passes: every other
    part of the check needs Redis too — ``_resolve_role`` cannot read the role
    — so the call is about to be denied anyway, and blocking here would only
    replace a clear error with a confusing one.

    Anything else fails **closed**. The distinction matters because the root
    document can be unreadable while Redis is perfectly healthy: ``get_root``
    catches only ``redis.RedisError``, so a malformed ``meta:root`` raises
    ``JSONDecodeError`` straight through. Under the old blanket
    ``except Exception: pass`` that was a silent skip — one corrupt key and
    writes were accepted on a read-only replica, with the role check passing
    normally because it reads a different key. An unverifiable flag is not an
    absent flag.
    """
    try:
        root = RedisCatalog().get_root(organization, super_name)
    except _redis.RedisError:
        return  # Redis is down; the role lookup below will fail too.
    except PermissionError:
        raise
    except Exception as e:
        logger.error(
            "[readonly-guard] cannot determine read-only state of %s/%s: %s "
            "— refusing rather than assuming writable",
            organization, super_name, e,
        )
        raise PermissionError(
            f"Cannot verify whether this SuperTable is read-only. Cannot {label}."
        )

    if root and root.get("read_only"):
        clone_type = root.get("clone_type", "")
        if clone_type == "replica":
            reason = "a live replica"
        elif clone_type == "readonly":
            reason = "a read-only snapshot clone"
        elif root.get("cloned_from"):
            reason = "a read-only clone"
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
    Check whether *role_name* is allowed to perform a CONTROL operation on
    *table_name* — dropping the whole SuperTable, and nothing else.

    Dropping a single *table* is WRITE, not CONTROL: a writer owns the tables
    it was granted and can create and drop them, bounded by its table grants.

    Raises ``PermissionError`` if the role lacks the necessary permission.
    """
    _check_readonly_guard(super_name, organization, "control this table")
    _check_operation_access(
        super_name, organization, role_name, table_name,
        Permission.CONTROL, "control this table",
    )


def check_rbac_access(
    super_name: str,
    organization: str,
    role_name: str,
) -> None:
    """Check whether *role_name* may administer roles and users.

    Requires :attr:`Permission.RBAC`, held by ``SUPERADMIN`` and ``ADMIN``.
    Not table-scoped: a role grants access to tables, so checking "which
    table is this role change about" has no answer.

    Raises ``PermissionError`` if the role may not administer access.
    """
    _check_readonly_guard(super_name, organization, "administer roles and users")
    _check_scope_access(
        super_name, organization, role_name,
        Permission.RBAC, "administer roles and users",
    )


def check_write_access(
    super_name: str,
    organization: str,
    role_name: str,
    table_name: str,
) -> None:
    """
    Check whether *role_name* is allowed to WRITE to *table_name*.

    Raises ``PermissionError`` if the role lacks the necessary permission.
    """
    _check_readonly_guard(super_name, organization, "write to this table")
    _check_operation_access(
        super_name, organization, role_name, table_name,
        Permission.WRITE, "write to this table",
    )


def check_meta_access(
    super_name: str,
    organization: str,
    role_name: str,
    table_name: str,
) -> None:
    """
    Check whether *role_name* may read metadata for *table_name* — schemas,
    statistics, and table/SuperTable listings. No row data.

    Deliberately does NOT call :func:`_check_readonly_guard`. META is a pure
    read: every call site in the tree reads and none mutates, so the guard
    could only ever produce a false denial. On a read-only snapshot clone or
    a live replica it did exactly that — ``SELECT`` succeeded through
    :func:`restrict_read_access` while listing tables, reading a schema or
    fetching stats all failed with "Cannot modify metadata". Worse, the
    listing helpers filter ``PermissionError`` per item, so the replica did
    not error: it silently appeared empty.

    The name is historical. It was once described as an ALTER-style
    operation, which is what put the mutation guard here.

    Raises ``PermissionError`` if the role lacks the necessary permission.
    """
    _check_operation_access(
        super_name, organization, role_name, table_name,
        Permission.META, "META data",
    )


def restrict_read_access(
        super_name: str,
        organization: str,
        role_name: str,
        tables: List[TableDefinition],
        physical_tables: List[TableDefinition],
) -> Dict[str, "RbacViewDef"]:
    """
    Check whether *role_name* can read the requested tables/columns.

    Validation uses *physical_tables* (CTE-free, merged by
    ``SQLParser.get_physical_tables()``) to check table and column
    access against the role's per-table definitions.

    View definitions are built per *alias* (from *tables* /
    ``SQLParser.get_table_tuples()``) so the engine can create one
    filtered view per alias.

    Returns a dict of ``{alias: RbacViewDef}`` for each alias that needs
    RBAC filtering.  An empty dict means the role is unrestricted
    (superadmin/admin or wildcard columns+filters on all tables).

    Raises ``PermissionError`` if the role lacks READ access entirely,
    if a requested table is not in the role's allowed table set, or if
    a requested column is not in the role's per-table allowed columns.

    Role table definition format::

        role_info["tables"] = {
            "*": {"columns": ["*"], "filters": ["*"]},       # default
            "orders": {"columns": ["id", "amount"], "filters": [...]},
            "customers": {"columns": ["*"], "filters": ["*"]},
        }
    """
    from supertable.data_classes import RbacViewDef

    role_manager = RoleManager(super_name=super_name, organization=organization)
    role_info = _resolve_role(role_manager, role_name)

    role_type_str = role_info.get("role")
    if not role_type_str:
        raise PermissionError("You don't have permission to read the table.")

    try:
        role_type = RoleType(role_type_str)
    except ValueError:
        raise PermissionError("You don't have permission to read the table.")

    if not has_permission(role_type, Permission.READ):
        raise PermissionError("You don't have permission to read the table.")

    # Superadmin/admin: no filtering needed
    if role_type in (RoleType.SUPERADMIN, RoleType.ADMIN):
        return {}

    role_tables = _normalize_tables(role_info.get("tables", {}))
    default_entry = role_tables.get("*")

    # Fast path: if the only entry is "*" with all-wildcards, no views needed
    if (
        default_entry
        and len(role_tables) == 1
        and default_entry.get("columns", ["*"]) == ["*"]
        and default_entry.get("filters", ["*"]) == ["*"]
    ):
        return {}

    # ── Phase 1: Validate physical tables (merged, CTE-free) ──────── #
    #
    # This validates the *complete* set of physical columns the query
    # touches per table — columns from SELECT, WHERE, JOIN ON, GROUP BY,
    # HAVING, window functions, ORDER BY — all merged and deduplicated
    # by get_physical_tables().

    for pt in physical_tables:
        table_entry = _resolve_table_entry(role_tables, pt.simple_name)
        if table_entry is None:
            raise PermissionError(
                f"You don't have permission to read table '{pt.simple_name}'."
            )

        allowed_columns = normalize_allowed_columns(table_entry.get("columns"))
        if allowed_columns == ["*"]:
            # Unrestricted columns for this table — skip column validation.
            continue

        if not allowed_columns:
            raise PermissionError(
                f"You don't have permission to read any columns in '{pt.simple_name}'."
            )

        # pt.columns is [] when SELECT * or t.* was used.
        # In that case we cannot enumerate the requested columns at parse
        # time — the RBAC view will enforce column projection at execution.
        if pt.columns:
            requested_lower = {c.lower() for c in pt.columns}
            allowed_lower = {c.lower() for c in allowed_columns}
            denied = requested_lower - allowed_lower
            if denied:
                raise PermissionError(
                    f"You don't have permission to columns: {denied} "
                    f"in table '{pt.simple_name}'."
                )

    # ── Phase 2: Build per-alias RbacViewDef ──────────────────────── #
    #
    # Each alias gets its own view definition derived from its physical
    # table's entry in the role.  CTE aliases (which have no physical
    # table entry and no "*" default) are silently skipped — they have
    # already been validated transitively via physical_tables.

    rbac_views: Dict[str, RbacViewDef] = {}

    for td in tables:
        table_entry = _resolve_table_entry(role_tables, td.simple_name)
        if table_entry is None:
            # Not a physical table in the role (CTE alias) — the engine
            # will also skip this alias since it has no snapshot.
            continue

        allowed_columns = normalize_allowed_columns(table_entry.get("columns"))
        filters = table_entry.get("filters", ["*"])

        # Row-level filtering from per-table filters
        where_clause = ""
        if filters != ["*"]:
            try:
                fb = FilterBuilder(
                    table_name="__PLACEHOLDER__",
                    columns=["*"],
                    role_info={"filters": filters},
                )
            except Exception as exc:
                # A filter that cannot be rendered is a broken policy.  Serving
                # the table unfiltered would turn a rendering bug straight into
                # a missing control, so deny instead (S9/S6).  The catch is
                # deliberately broad: a malformed filter document raises
                # TypeError/KeyError rather than ValueError, and *every* way of
                # failing to render a configured restriction has to land on
                # "deny", never on "no filter".
                logger.error(
                    f"Role '{role_name}' has an unusable row filter on "
                    f"'{td.simple_name}': {exc}"
                )
                raise PermissionError(
                    f"You don't have permission to read table "
                    f"'{td.simple_name}'."
                ) from exc

            generated = fb.filter_query
            where_idx = generated.upper().find("WHERE ")
            if where_idx >= 0:
                where_clause = generated[where_idx + 6:]

            if not where_clause:
                # Belt and braces: the decision to skip the view must be driven
                # by the *policy* (is it restrictive?), never by whether we
                # managed to render it.
                logger.error(
                    f"Role '{role_name}' has a non-wildcard row filter on "
                    f"'{td.simple_name}' that produced no WHERE clause"
                )
                raise PermissionError(
                    f"You don't have permission to read table "
                    f"'{td.simple_name}'."
                )

        # Only add an entry if there's actual filtering to apply
        if allowed_columns != ["*"] or where_clause:
            rbac_views[td.alias] = RbacViewDef(
                allowed_columns=list(allowed_columns),
                where_clause=where_clause,
            )

    return rbac_views
