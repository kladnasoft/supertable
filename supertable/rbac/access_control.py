# supertable/rbac/access_control.py

import json as _json
from typing import Any, Dict, List, Tuple

import redis as _redis

from supertable.config.defaults import logger
from supertable.data_classes import TableDefinition
# Imported at module scope, not per call: ``_check_readonly_guard`` runs on
# every write, and both of these were function-local — a needless sys.modules
# lookup and frame setup on the hot path. No cycle: ``redis_catalog`` imports
# ``rbac.permissions`` (a leaf) and never imports this module.
from supertable.redis_catalog import RedisCatalog
from supertable.rbac import role_manager as _role_manager
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.permissions import has_permission, Permission, RoleType
from supertable.rbac.filter_builder import FilterBuilder
from supertable.rbac.row_column_security import normalize_allowed_columns
from supertable.utils.sql_parser import SQLParser


def _resolve_role(role_manager: RoleManager, role_name: str) -> dict:
    """Resolve a role_name to its role document.

    Raises ``PermissionError`` if the role does not exist or is disabled.
    """
    return _usable_role(role_manager.get_role_by_name(role_name), role_name)


def _usable_role(role_info: dict, role_name: str) -> dict:
    """The role document read for *role_name*, if it is one that can be used.

    The half of :func:`_resolve_role` that follows the lookup, for a caller
    that has read the document some other way.

    Raises ``PermissionError`` if the role does not exist or is disabled.
    """
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
    _require_permission(role_info, role_name, permission, label)
    return role_info


def _require_permission(
    role_info: dict,
    role_name: str,
    permission: Permission,
    label: str,
) -> None:
    """Require that the role document's type holds *permission*.

    The half of :func:`_check_scope_access` that follows the role's
    resolution.  Raises ``PermissionError`` for a document without a role
    type, with one that is no ``RoleType``, or whose type lacks *permission*.
    """
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
        raise _unverifiable_root(super_name, organization, label, e)

    _refuse_if_read_only(root, label)


def _unverifiable_root(
    super_name: str, organization: str, label: str, error: Exception,
) -> PermissionError:
    """Log that the lake's read-only state cannot be told; return the refusal.

    The fail-closed half of :func:`_check_readonly_guard`, for its two
    readers of the root document.  The caller raises what is returned.
    """
    logger.error(
        "[readonly-guard] cannot determine read-only state of %s/%s: %s "
        "— refusing rather than assuming writable",
        organization, super_name, error,
    )
    return PermissionError(
        f"Cannot verify whether this SuperTable is read-only. Cannot {label}."
    )


def _refuse_if_read_only(root, label: str) -> None:
    """Raise ``PermissionError`` when *root* marks the lake as read-only.

    What :func:`_check_readonly_guard` decides once it has the root document.
    """
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


#: The functions of this module the one read stands for, as the module
#: defines them.  A caller that has put another in the place of one of them
#: (a test does, to switch a check off or to watch it) is asked the one it
#: put there: see ``_checks_are_this_modules``.
_ONE_READ_STANDS_FOR = (
    _check_readonly_guard,
    _check_operation_access,
    _check_scope_access,
    _resolve_role,
)


def _checks_are_this_modules() -> bool:
    """Whether the guard and the role's resolution are still this module's."""
    return (
        _check_readonly_guard,
        _check_operation_access,
        _check_scope_access,
        _resolve_role,
    ) == _ONE_READ_STANDS_FOR


def _guard_and_role_in_one_read(
    super_name: str,
    organization: str,
    role_name: str,
    label: str,
):
    """The read-only guard and the role's lookup, on one read of Redis.

    A mutation's access check read Redis five times, one round trip each: the
    lake's root for :func:`_check_readonly_guard`, two checks that the role
    store is initialised when ``RoleManager`` is constructed, the role name's
    id and the role's document.  ``RoleManager.read_for_access_check`` reads
    the five at once.

    Returns the role document for *role_name* (``{}`` when the name stands
    for no role), with the guard already applied to the root read with it:
    ``PermissionError`` is raised here for a lake that is read-only, or whose
    root document cannot be read as one, exactly as the guard raises it.

    Returns ``None`` when that one read is not to be had, and the caller then
    checks as it always did, with the guard's own read and a constructed
    ``RoleManager``: the role store is not initialised, the collaborators are
    not the ones that know the read (an older catalog, a test double in the
    place of ``RoleManager`` or ``RedisCatalog``, another function in the
    place of this module's guard or role resolution), or the read failed.  A
    read that failed says nothing about the lake: the guard's own read follows
    it, with the failure handling the guard has.
    """
    if not _checks_are_this_modules():
        return None
    # The guard read the root through this module's catalog and the role came
    # from the catalog a manager builds for itself.  The one read asks one
    # catalog for both, so the two have to be one class: with another put in
    # the place of either, each is asked what it was asked before.
    if getattr(_role_manager, "RedisCatalog", None) is not RedisCatalog:
        return None
    try:
        reader = getattr(RoleManager, "read_for_access_check", None)
        if not callable(reader):
            return None
        answer = reader(
            super_name=super_name,
            organization=organization,
            role_name=role_name,
            redis_catalog=RedisCatalog(),
        )
    except Exception:
        return None
    if type(answer) is not tuple or len(answer) != 2:
        return None
    root_text, role_info = answer
    if not (root_text is None or type(root_text) is str) or type(role_info) is not dict:
        return None

    try:
        # As RedisCatalog.get_root makes the document of the stored text.
        root = _json.loads(root_text) if root_text else None
    except Exception as e:
        raise _unverifiable_root(super_name, organization, label, e)
    _refuse_if_read_only(root, label)
    return role_info


def _role_in_one_read(super_name: str, organization: str, role_name: str):
    """The role document for *role_name*, on one read of Redis.

    A check without a read-only guard (a query) read Redis four times for its
    role, one round trip each: two checks that the role store is initialised
    when ``RoleManager`` is constructed, the role name's id and the role's
    document.  ``RoleManager.read_for_access_check`` reads the four at once,
    and leaves the lake's root alone.

    Returns the role document (``{}`` when the name stands for no role).

    Returns ``None`` when that one read is not to be had, and the caller then
    constructs a ``RoleManager`` and asks it, as it always did: the role store
    is not initialised (the constructor then makes the default role), Redis
    does not run the read's script, ``RoleManager`` is not the class that
    knows the read (a test double, a subclass), this module's role resolution
    has had another function put in its place, or the read failed.
    """
    if not _checks_are_this_modules():
        return None
    try:
        reader = getattr(RoleManager, "read_for_access_check", None)
        if not callable(reader):
            return None
        answer = reader(
            super_name=super_name,
            organization=organization,
            role_name=role_name,
            with_root=False,
        )
    except Exception:
        return None
    if type(answer) is not tuple or len(answer) != 2:
        return None
    role_info = answer[1]
    if type(role_info) is not dict:
        return None
    return role_info


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

    One round trip to Redis where there were five: the lake's root and the
    role are read together (:func:`_guard_and_role_in_one_read`), and the
    checks made on them are the ones below, in the same order.  When that
    read is not to be had, the guard and the role's lookup read Redis one
    after the other, as before.
    """
    label = "write to this table"
    role_info = _guard_and_role_in_one_read(super_name, organization, role_name, label)
    if role_info is None:
        _check_readonly_guard(super_name, organization, label)
        _check_operation_access(
            super_name, organization, role_name, table_name,
            Permission.WRITE, label,
        )
        return

    # The read-only guard has passed on the root read with the role.  What
    # follows is _check_operation_access on a role document already read.
    role_info = _usable_role(role_info, role_name)
    _require_permission(role_info, role_name, Permission.WRITE, label)
    role_tables = _normalize_tables(role_info.get("tables", {}))
    _check_table_access(role_tables, table_name, label)


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

    # The role, on one read of Redis where there were four; when that read is
    # not to be had, from a constructed RoleManager, as before.  Either way it
    # is the same document, put to the same test of existence and of being
    # enabled.
    role_info = _role_in_one_read(super_name, organization, role_name)
    if role_info is None:
        role_manager = RoleManager(super_name=super_name, organization=organization)
        role_info = _resolve_role(role_manager, role_name)
    else:
        role_info = _usable_role(role_info, role_name)

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


# ---------------------------------------------------------------------------
# Table-policy enforcement for platform callers
#
# These were public in 2.5.x and removed in 3.4.0, but the decisions they make
# are still needed by any service that authorizes an object without building a
# SQL view for it -- staging files, catalog entries, metadata pages.  They are
# restored here rather than reimplemented by the caller: an authorization rule
# that exists in two places eventually disagrees with itself.  Each one is a
# thin composition over the internals above, so there is one implementation of
# "does this role reach this table/column" in the process.
# ---------------------------------------------------------------------------

import hashlib as _hashlib
from dataclasses import dataclass as _dataclass
from typing import Iterable as _Iterable, Optional as _Optional


@_dataclass(frozen=True)
class RoleAccessContext:
    """One fully validated role policy for one SuperTable namespace."""

    role_info: dict
    role_type: RoleType
    tables: Dict[str, dict]
    fingerprint: str


def _entry_allows_access(entry: _Optional[dict]) -> bool:
    return isinstance(entry, dict) and entry.get("access", "allow") != "deny"


def _requested_columns_denied(
    entry: dict,
    columns: _Optional[_Iterable[str]],
    table_name: str,
) -> None:
    """Case-insensitive include-minus-exclude, matching the read path."""
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


def resolve_role_access_context(
    super_name: str,
    organization: str,
    role_name: str,
    permission: Permission = Permission.META,
    label: str = "META data",
) -> RoleAccessContext:
    """Validate a role once and return its policy plus a stable fingerprint.

    The fingerprint covers role id, type, enabled flag and the whole table map,
    so a caller can cache against it and have the cache fall out of date the
    moment the grant changes.
    """
    role_manager = RoleManager(super_name=super_name, organization=organization)
    role_info = _resolve_role(role_manager, role_name)
    raw_type = role_info.get("role")
    role_type: _Optional[RoleType] = None
    if isinstance(raw_type, str):
        try:
            role_type = RoleType(raw_type)
        except (TypeError, ValueError):
            role_type = None
    if role_type is None:
        logger.error("Persisted role type is invalid")
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
    fingerprint = _hashlib.sha256(
        _json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    return RoleAccessContext(role_info, role_type, tables, fingerprint)


def resolve_table_policy(
    context: RoleAccessContext,
    table_name: str,
    permission_label: str = "access",
) -> dict:
    """Return the effective policy for one table, or deny it.

    SUPERADMIN is the unconditional control-plane bypass.  ADMIN keeps every
    operation permission but an explicitly scoped ADMIN still honours the same
    table and column denies as any other role.
    """
    if context.role_type is RoleType.SUPERADMIN:
        return {"columns": ["*"], "filters": ["*"]}
    entry = _resolve_table_entry(context.tables, table_name)
    if not _entry_allows_access(entry):
        raise PermissionError(
            f"You don't have permission to {permission_label} table '{table_name}'."
        )
    return entry


def check_requested_columns(
    entry: dict,
    columns: _Optional[_Iterable[str]],
    table_name: str,
) -> None:
    """Enforce one resolved table policy against requested columns.

    Obtain ``entry`` from :func:`resolve_table_policy` first.
    """
    _requested_columns_denied(entry, columns, table_name)


def visible_columns_for_policy(
    entry: dict,
    available_columns: _Iterable[str],
) -> List[str]:
    """Resolve include-minus-exclude against the schema's actual spellings."""
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


def check_read_access(
    super_name: str,
    organization: str,
    role_name: str,
    table_name: str,
    columns: _Optional[_Iterable[str]] = None,
    *,
    require_unfiltered: bool = False,
) -> None:
    """Authorize a direct bounded read without constructing SQL views.

    Most reads go through :func:`restrict_read_access` because they need row
    and column policy views.  Control-plane objects such as staging files have
    no SQL table of their own, but exposing their row values still requires
    READ, never merely META.  ``require_unfiltered`` refuses a scoped policy
    outright, because a staging object cannot have a row filter applied to it.
    """
    if type(require_unfiltered) is not bool:
        raise TypeError("require_unfiltered must be a boolean")
    context = resolve_role_access_context(
        super_name, organization, role_name,
        Permission.READ, "read this table",
    )
    entry = resolve_table_policy(context, table_name, "read")
    _requested_columns_denied(entry, columns, table_name)
    if require_unfiltered and (
        entry.get("columns", ["*"]) != ["*"]
        or bool(entry.get("exclude_columns"))
        or entry.get("filters", ["*"]) != ["*"]
    ):
        raise PermissionError(
            f"Scoped table policy for '{table_name}' cannot read an "
            "unfiltered staging object."
        )


def check_create_access(
    super_name: str,
    organization: str,
    role_name: str,
    table_name: str,
    columns: _Optional[_Iterable[str]] = None,
) -> None:
    """Authorize creating a table, plus the target table/column policy.

    3.4.0's ``Permission`` enum has no CREATE member -- creation is authorized
    as WRITE, which is what ``SimpleTable`` itself checks.
    """
    _check_readonly_guard(super_name, organization, "create this table")
    context = resolve_role_access_context(
        super_name, organization, role_name,
        Permission.WRITE, "create this table",
    )
    entry = resolve_table_policy(context, table_name, "create")
    _requested_columns_denied(entry, columns, table_name)
