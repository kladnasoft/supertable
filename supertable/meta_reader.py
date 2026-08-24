import os
import copy
import hashlib

from supertable.config.settings import settings
import logging
import json
import time
import threading
from datetime import datetime
from typing import List, Optional, Dict, Any, Set, Tuple

from supertable.errors import SuperTableNotFoundError, TableNotFoundError
from supertable.rbac.access_control import (
    check_meta_access,
    visible_columns_for_policy,
)
from supertable.rbac.permissions import RoleType
from supertable.redis_catalog import RedisCatalog
from supertable import redis_keys as RK

from supertable.super_table import SuperTable
from supertable.simple_table import SimpleTable
from supertable.tombstone_manifest_v2 import (
    TombstoneManifestV2Error,
    normalize_snapshot_tombstone_state,
)
from supertable.utils.snapshot import complete_snapshot_payload
from supertable.row_identity import snapshot_proves_stable_rowids

logger = logging.getLogger(__name__)


# Small in-process cache to de-duplicate bursty reflection calls.
_SUPER_META_CACHE: Dict[str, Tuple[int, float, Dict[str, Any]]] = {}
_SUPER_META_CACHE_LOCK = threading.Lock()
_SUPER_META_CACHE_MAX_ENTRIES = 256

_HIDDEN_COLUMNS = frozenset({
    "__rowid__",
    "__timestamp__",
    "__file__",
    "__supertable_source_file__",
    "__supertable_scan_filename__",
})

def _super_meta_cache_ttl_s() -> float:
    val = settings.SUPERTABLE_SUPER_META_CACHE_TTL_S
    if not val:
        return 1.0
    try:
        ttl = float(val)
    except Exception:
        return 1.0
    return max(0.0, ttl)


def _prune_dict(d: Dict[str, Any], keys_to_remove: Set[str]) -> Dict[str, Any]:
    """Return a shallow copy of d with selected keys removed (non-mutating)."""
    return {k: v for k, v in d.items() if k not in keys_to_remove}


def _visible_schema(schema_obj: Any, entry: dict) -> Dict[str, Any]:
    schema = _schema_to_dict(schema_obj)
    candidates = [
        name for name in schema
        if str(name).casefold() not in _HIDDEN_COLUMNS
    ]
    visible = set(visible_columns_for_policy(entry, candidates))
    return {name: schema[name] for name in schema if name in visible}


def _has_row_filter(entry: dict) -> bool:
    """Whether metadata aggregates cannot be represented without a data scan.

    Snapshot/resource counters describe the complete physical table.  Returning
    them for a row-filtered role leaks facts about rows outside that role's
    predicate.  Metadata APIs therefore omit such tables rather than presenting
    the unfiltered counters as though they were policy-scoped.
    """
    return entry.get("filters", ["*"]) != ["*"]


def _document_has_share_row_filter(document: object) -> bool:
    """Conservatively recognize an effective linked-share row predicate.

    ``_row_filter`` is trusted authorization metadata, not response data.  A
    malformed non-null value therefore fails closed just like a valid
    predicate; only an absent, null, or blank-string marker is unrestricted.
    """
    if not isinstance(document, dict) or "_row_filter" not in document:
        return False
    raw_filter = document.get("_row_filter")
    if raw_filter is None:
        return False
    if isinstance(raw_filter, str):
        return bool(raw_filter.strip())
    return True


def _metadata_has_share_row_filter(document: object) -> bool:
    """Inspect the supported leaf/snapshot wrappers without exposing a filter.

    Linked-share overlays have appeared both beside ``payload`` and inside its
    nested ``snapshot``.  Restrict traversal to those metadata wrappers so a
    user schema containing a column literally named ``_row_filter`` does not
    become a false policy marker.
    """
    if not isinstance(document, dict):
        return False
    candidates = [document]
    for wrapper_name in ("payload", "data", "snapshot"):
        wrapped = document.get(wrapper_name)
        if isinstance(wrapped, dict):
            candidates.append(wrapped)
            nested = wrapped.get("snapshot")
            if isinstance(nested, dict):
                candidates.append(nested)
    return any(_document_has_share_row_filter(item) for item in candidates)


def _complete_leaf_snapshot(leaf: object) -> Optional[Dict[str, Any]]:
    """Return the authoritative cached snapshot selected by a Redis leaf.

    A merely snapshot-shaped payload is not enough: legacy/partial caches can
    omit deletion state and linked-share authorization metadata.  Callers must
    resolve the immutable document whenever this validator returns ``None``.
    """
    if not isinstance(leaf, dict):
        return None
    return complete_snapshot_payload(
        leaf.get("payload"),
        expected_version=leaf.get("version"),
        require_policy_marker=True,
    )


def _leaf_has_complete_policy_context(leaf: object) -> bool:
    """Whether Redis alone proves the selected snapshot's policy metadata."""
    return _complete_leaf_snapshot(leaf) is not None


def _validated_live_row_count(
    snapshot: Dict[str, Any],
    physical_rows: int,
) -> int:
    """Return exact live rows without coercing malformed DV state to zero."""
    try:
        state = normalize_snapshot_tombstone_state(snapshot)
    except (TypeError, TombstoneManifestV2Error) as exc:
        raise RuntimeError("Snapshot has an invalid deletion-vector state") from exc

    if state.pointer is None:
        return physical_rows
    deleted_rows = state.rows
    if deleted_rows > physical_rows:
        raise RuntimeError(
            "Snapshot deletion-vector rows exceed physical resource rows"
        )
    return physical_rows - deleted_rows


def _nonnegative_metadata_int(value: object) -> Optional[int]:
    """Return an exact non-negative integer, never a coercible lookalike."""
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


def _complete_resource_metric(
    resources: object,
    field_name: str,
) -> Optional[int]:
    """Sum one sealed resource metric only when every entry is complete."""
    if not isinstance(resources, list):
        return None
    total = 0
    for resource in resources:
        if not isinstance(resource, dict):
            return None
        value = _nonnegative_metadata_int(resource.get(field_name))
        if value is None:
            return None
        total += value
    return total


def _sanitize_snapshot_stats(
    snapshot: Dict[str, Any],
    entry: dict,
    *,
    table_name: str,
) -> Dict[str, Any]:
    """Project a snapshot to safe, policy-aware aggregate metadata.

    Snapshot resource dictionaries are storage control data: their ``file``
    values may be local object keys or linked-share bearer URLs, and future
    adapters can attach credentials or cached manifests beside them. A deny
    list is therefore not a durable response boundary. Return only a fixed
    set of scalar aggregates derived from the pinned snapshot.
    """
    if not isinstance(snapshot, dict):
        raise RuntimeError("Simple table snapshot is invalid")

    result: Dict[str, Any] = {"simple_name": table_name}
    for field_name in ("snapshot_version", "last_updated_ms"):
        value = _nonnegative_metadata_int(snapshot.get(field_name))
        if value is not None:
            result[field_name] = value

    visible_schema = _visible_schema(snapshot.get("schema", {}), entry)
    result["cols_count"] = len(visible_schema)

    resources = snapshot.get("resources")
    if isinstance(resources, list):
        result["files"] = len(resources)

    total_size = _complete_resource_metric(resources, "file_size")
    if total_size is not None:
        result["size"] = total_size

    physical_rows = _complete_resource_metric(resources, "rows")
    if physical_rows is not None:
        try:
            result["rows"] = _validated_live_row_count(
                snapshot,
                physical_rows,
            )
        except RuntimeError:
            # Malformed or incomplete deletion state makes the row count
            # unknown. Other aggregates remain useful and contain no paths.
            logger.warning(
                "[get_table_stats] omitting rows for table %s because "
                "deletion metadata is invalid",
                table_name,
            )

    return result


def _checked_meta_context(
    *, super_name: str, organization: str, role_name: str, table_name: str,
) -> Tuple[Any, dict]:
    """Run the public META gate and reuse its validated policy context.

    Older integrations (and test doubles) return ``None`` from the gate.  That
    return shape predates column policies; treating an already-successful
    legacy gate as unrestricted is backwards-compatible.  Production's gate
    now returns the exact context+entry, so no real request can take this path.
    """
    resolved = check_meta_access(
        super_name=super_name,
        organization=organization,
        role_name=role_name,
        table_name=table_name,
    )
    if (
        isinstance(resolved, tuple)
        and len(resolved) == 2
        and hasattr(resolved[0], "role_type")
        and isinstance(resolved[1], dict)
    ):
        return resolved
    context = type("LegacyMetaContext", (), {
        "role_type": RoleType.SUPERADMIN,
        "role_info": {"role_id": role_name},
        "tables": {"*": {"columns": ["*"], "filters": ["*"]}},
        "fingerprint": "legacy-meta-gate",
    })()
    return context, {"columns": ["*"], "filters": ["*"]}

def _get_redis_items(pattern) -> List[str]:
    """
    Get all tables for this super table by scanning Redis keys.
    """
    catalog = RedisCatalog()
    items = []
    cursor = 0
    while True:
        cursor, keys = catalog.r.scan(cursor=cursor, match=pattern, count=1000)
        for key in keys:
            # Decode the catalog key strictly. A malformed key is corrupt
            # metadata, not an empty namespace.
            if isinstance(key, bytes):
                key_str = key.decode("utf-8")
            elif isinstance(key, str):
                key_str = key
            else:
                raise RuntimeError("Redis SCAN returned an invalid key type")

            items.append(key_str)
        if cursor == 0:
            break
    return items




def _try_parse_leaf_meta(raw: Any) -> Optional[Dict[str, Any]]:
    """Strictly decode one Redis leaf, returning ``None`` only if absent."""
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw_s = bytes(raw).decode("utf-8")
    elif isinstance(raw, str):
        raw_s = raw
    else:
        raise RuntimeError("Redis leaf value has an invalid type")
    raw_s = raw_s.strip()
    if not raw_s:
        raise RuntimeError("Redis leaf value is empty")
    try:
        parsed = json.loads(raw_s)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Redis leaf value is not valid JSON") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("Redis leaf value is not a JSON object")
    return parsed


def _schema_to_dict(schema_obj: Any) -> Dict[str, Any]:
    """Normalize schema representations into a {name: type} dict."""
    if isinstance(schema_obj, dict):
        return schema_obj
    if isinstance(schema_obj, list):
        out: Dict[str, Any] = {}
        for item in schema_obj:
            if isinstance(item, dict):
                # Common form: [{name,type}, ...]
                name = item.get("name")
                if name is not None:
                    out[str(name)] = item.get("type")
                    continue
                # Fallback: single-key dict
                if len(item) == 1:
                    k = next(iter(item.keys()))
                    out[str(k)] = item.get(k)
        return out
    return {}



class MetaReader:
    """
    Read-only metadata helper for SuperTable & SimpleTable.
    Optimized for Redis-based metadata with minimal locking.
    """

    def __init__(self, super_name: str, organization: str):
        # ``MetaReader`` is read-only by contract — opening one against a
        # missing supertable must surface ``SuperTableNotFoundError``
        # rather than silently bootstrap an empty supertable as a side
        # effect of constructing this object.
        self.super_table = SuperTable(
            super_name=super_name,
            organization=organization,
            create_if_missing=False,
        )
        self.catalog = RedisCatalog()


    def _get_all_tables(self) -> List[str]:
        """Get one lifecycle-pinned, replica-aware set of valid leaf names."""
        tables = []
        seen = set()
        for item in self.catalog.scan_leaf_items(
            self.super_table.organization,
            self.super_table.super_name,
            count=1000,
        ):
            if not isinstance(item, dict):
                raise RuntimeError("Catalog leaf scan returned an invalid item")
            table_name = item.get("simple")
            if not isinstance(table_name, str) or not table_name:
                raise RuntimeError("Catalog leaf scan returned an invalid table name")
            try:
                RK.meta_leaf(
                    self.super_table.organization,
                    self.super_table.super_name,
                    table_name,
                )
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    "Catalog leaf scan returned an invalid table name"
                ) from exc
            self.catalog.check_deletion_intent_absent(
                self.super_table.organization,
                self.super_table.super_name,
                simple=table_name,
            )
            if (
                not (
                    table_name.startswith("__")
                    and table_name.endswith("__")
                )
                and table_name not in seen
            ):
                seen.add(table_name)
                tables.append(table_name)
        return tables

    def _authorized_meta_targets(
        self, table_name: str, role_name: str,
    ) -> Tuple[Optional[Any], List[Tuple[str, dict]]]:
        """Authorize children before exposing an aggregate parent.

        Inclusion-only roles commonly have entries for child tables but no
        synthetic entry named after the SuperTable.  Checking that synthetic
        parent first therefore denies a valid metadata view.  Resolve each real
        child independently and expose the aggregate only when at least one
        child remains visible.
        """
        aggregate = (
            str(table_name).casefold()
            == str(self.super_table.super_name).casefold()
        )
        targets = self._get_all_tables() if aggregate else [table_name]
        authorized: List[Tuple[str, dict]] = []
        context = None
        for target in targets:
            try:
                target_context, entry = _checked_meta_context(
                    super_name=self.super_table.super_name,
                    organization=self.super_table.organization,
                    role_name=role_name,
                    table_name=target,
                )
            except PermissionError:
                continue
            if context is None:
                context = target_context
            authorized.append((target, entry))

        # With no physical children there is nothing to expose, but still run
        # the role/type META gate so an invalid role is not indistinguishable
        # from an empty authorized namespace in logs and tests.
        if aggregate and not targets:
            try:
                context, _ = _checked_meta_context(
                    super_name=self.super_table.super_name,
                    organization=self.super_table.organization,
                    role_name=role_name,
                    table_name=table_name,
                )
            except PermissionError:
                context = None
        return context, authorized

    def get_tables(self, role_name: str) -> List[str]:
        tables = self._get_all_tables()
        result = []
        for table in tables:
            try:
                check_meta_access(super_name=self.super_table.super_name, organization=self.super_table.organization,
                              role_name=role_name, table_name=table)
                result.append(table)
            except PermissionError:
                logger.warning(f"No permission for the user: {role_name} to table: {table}")

        return result

    def get_table_schema(self, table_name: str, role_name: str) -> Optional[List[Dict[str, Any]]]:
        context, authorized = self._authorized_meta_targets(
            table_name, role_name,
        )
        if context is None or not authorized:
            logger.warning(
                "[get_table_schema] No visible metadata for user '%s' on table '%s'",
                role_name, table_name,
            )
            return None

        schema_items: Set[Tuple[str, Any]] = set()
        targets = [target for target, _entry in authorized]
        table_policies = dict(authorized)
        if len(targets) == 1 and table_name != self.super_table.super_name:
            raws = [self.catalog.r.get(RK.meta_leaf(
                self.super_table.organization,
                self.super_table.super_name,
                targets[0],
            ))]
        else:
            leaf_keys = [
                RK.meta_leaf(
                    self.super_table.organization,
                    self.super_table.super_name,
                    target,
                )
                for target in targets
            ]
            raws = self.catalog.r.mget(leaf_keys) if leaf_keys else []
        if len(raws) != len(targets):
            raise RuntimeError("Redis MGET returned an incomplete leaf response")

        for index, target in enumerate(targets):
            entry = table_policies[target]
            try:
                raw = raws[index] if index < len(raws) else None
                leaf_meta = _try_parse_leaf_meta(raw)
                st_data = _complete_leaf_snapshot(leaf_meta)
                if st_data is None:
                    simple_table = SimpleTable(
                        self.super_table, target, create_if_missing=False,
                    )
                    st_data, _ = simple_table.get_simple_table_snapshot()
                if not isinstance(st_data, dict):
                    raise RuntimeError("Simple table snapshot is invalid")
                schema = _visible_schema((st_data or {}).get("schema", {}), entry)
                schema_items.update(schema.items())
            except (FileNotFoundError, TableNotFoundError) as e:
                logger.debug("Failed to read schema for table %s: %s", target, e)
                continue

        return [dict(sorted(schema_items))]

    def get_odata_table_schema(
        self, table_name: str, role_name: str,
    ) -> Optional[List[Dict[str, Any]]]:
        """Return one keyed OData-capable table schema, or fail closed.

        OData entity types require a stable unique key.  Only one direct local
        table whose exact pinned snapshot proves the modern table-global
        ``__rowid__`` writer contract is eligible.  The raw system column is
        never included in the returned role-visible schema.
        """
        if str(table_name).casefold() == str(
            self.super_table.super_name
        ).casefold():
            # The aggregate relation combines independently versioned child
            # tables and therefore has no one table-global row-id namespace.
            return None
        context, authorized = self._authorized_meta_targets(
            table_name, role_name,
        )
        if context is None or len(authorized) != 1:
            return None
        target, entry = authorized[0]
        if str(target).casefold() != str(table_name).casefold():
            return None

        try:
            raw = self.catalog.r.get(RK.meta_leaf(
                self.super_table.organization,
                self.super_table.super_name,
                target,
            ))
            leaf_meta = _try_parse_leaf_meta(raw)
            if leaf_meta is None:
                return None
            snapshot = _complete_leaf_snapshot(leaf_meta)
            if snapshot is None:
                simple_table = SimpleTable(
                    self.super_table, target, create_if_missing=False,
                )
                snapshot, _ = simple_table.get_simple_table_snapshot()
            if not snapshot_proves_stable_rowids(snapshot, leaf_meta):
                return None
            visible = _visible_schema(snapshot.get("schema", {}), entry)
            return [dict(sorted(visible.items()))] if visible else None
        except (FileNotFoundError, TableNotFoundError):
            return None

    def collect_simple_table_schema(self, schemas: set, table_name: str, role_name: str) -> None:
        try:
            context, entry = _checked_meta_context(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                role_name=role_name,
                table_name=table_name,
            )
        except PermissionError as e:
            logger.warning(
                "[collect_simple_table_schema] Access denied for user '%s' on table '%s': %s",
                role_name, table_name, str(e)
            )
            return

        try:
            simple_table = SimpleTable(
                self.super_table, table_name, create_if_missing=False,
            )
            simple_table_data, _ = simple_table.get_simple_table_snapshot()
        except (FileNotFoundError, TableNotFoundError):
            logger.debug("Simple table snapshot missing for %s", table_name)
            return

        schema = _visible_schema(simple_table_data.get("schema", {}) or {}, entry)
        schema_tuple = tuple(sorted(schema.items()))
        schemas.add(schema_tuple)

    def get_table_stats(self, table_name: str, role_name: str) -> List[Dict[str, Any]]:
        context, authorized = self._authorized_meta_targets(
            table_name, role_name,
        )
        if context is None or not authorized:
            logger.warning(
                "[get_table_stats] No visible metadata for user '%s' on table '%s'",
                role_name, table_name,
            )
            return []

        stats: List[Dict[str, Any]] = []
        for table, entry in authorized:
            if _has_row_filter(entry):
                logger.info(
                    "[get_table_stats] omitting unscoped counters for "
                    "row-filtered table %s",
                    table,
                )
                continue
            try:
                st = SimpleTable(
                    self.super_table, table, create_if_missing=False,
                )
                st_data, _ = st.get_simple_table_snapshot()
                # ``get_simple_table_snapshot`` can return the nested cached
                # snapshot and thereby omit an outer linked-share overlay.
                # Inspect both the exact leaf pinned by that read and the
                # resolved snapshot before returning any full-table metric or
                # physical resource path.
                if (
                    _metadata_has_share_row_filter(
                        getattr(st, "_last_snapshot_leaf", None),
                    )
                    or _metadata_has_share_row_filter(st_data)
                ):
                    logger.info(
                        "[get_table_stats] omitting unscoped counters for "
                        "linked-share-filtered table %s",
                        table,
                    )
                    continue
                stats.append(_sanitize_snapshot_stats(
                    st_data,
                    entry,
                    table_name=table,
                ))
            except (FileNotFoundError, TableNotFoundError):
                logger.debug("Simple table snapshot missing for %s", table)
                continue

        return stats

    def get_super_meta(self, role_name: str) -> Optional[Dict[str, Any]]:

        debug_timings = settings.SUPERTABLE_DEBUG_TIMINGS
        t0 = time.perf_counter()

        context, authorized = self._authorized_meta_targets(
            self.super_table.super_name, role_name,
        )
        # A SUPERADMIN may inspect an actually empty namespace. Scoped roles
        # still need at least one visible physical child; this preserves the
        # inclusion-only aggregate rule without making bootstrap diagnostics
        # disappear before the first table is created.
        empty_superadmin = bool(
            context is not None
            and context.role_type is RoleType.SUPERADMIN
            and not authorized
        )
        if context is None or (not authorized and not empty_superadmin):
            logger.warning(
                "[get_super_meta] No visible metadata for user '%s' on super '%s'",
                role_name, self.super_table.super_name,
            )
            return None

        t_access = time.perf_counter()

        root = self.catalog.get_root(org=self.super_table.organization, sup=self.super_table.super_name)
        t_root = time.perf_counter()

        # Physical snapshot metrics cannot be reduced by an RBAC row predicate
        # without executing the predicate against the table.  Treat those
        # tables like other non-visible metadata targets for this aggregate API;
        # schema/list APIs remain available through their dedicated methods.
        metric_authorized = [
            (table, entry) for table, entry in authorized
            if not _has_row_filter(entry)
        ]
        if authorized and not metric_authorized:
            logger.info(
                "[get_super_meta] no unfiltered metadata targets for role '%s'",
                role_name,
            )
            return None

        candidate_tables = [table for table, _entry in metric_authorized]
        candidate_policies: Dict[str, dict] = dict(metric_authorized)
        t_scan = time.perf_counter()

        # Resolve linked-share policy state before consulting the burst cache.
        # Root versions do not necessarily advance when a consumer leaf overlay
        # changes, so a cache keyed only by root+RBAC can otherwise replay raw
        # counters after a share predicate is attached.
        leaf_payloads: List[Optional[Dict[str, Any]]] = []
        leaf_keys: List[str] = [
            RK.meta_leaf(self.super_table.organization, self.super_table.super_name, t)
            for t in candidate_tables
        ]
        t_mget0 = time.perf_counter()
        raws = self.catalog.r.mget(leaf_keys) if leaf_keys else []
        if len(raws) != len(candidate_tables):
            raise RuntimeError("Redis MGET returned an incomplete leaf response")
        leaf_payloads = [_try_parse_leaf_meta(r) for r in raws]
        t_mget1 = time.perf_counter()

        cache_safe = len(raws) == len(candidate_tables)
        leaf_identity = hashlib.sha256()
        tables: List[str] = []
        table_policies: Dict[str, dict] = {}
        complete_leaf_snapshots: Dict[str, Dict[str, Any]] = {}
        linked_filter_omitted = False
        for index, table in enumerate(candidate_tables):
            raw = raws[index] if index < len(raws) else None
            leaf_identity.update(table.casefold().encode("utf-8"))
            leaf_identity.update(b"\0")
            if isinstance(raw, (bytes, bytearray)):
                leaf_identity.update(bytes(raw))
            elif raw is not None:
                leaf_identity.update(str(raw).encode("utf-8"))
            else:
                leaf_identity.update(b"<missing>")
                cache_safe = False
            leaf_identity.update(b"\0")

            leaf_meta = (
                leaf_payloads[index] if index < len(leaf_payloads) else None
            )
            if not isinstance(leaf_meta, dict):
                # A per-table SimpleTable fallback below can still recover the
                # snapshot, but the linked-share state is not yet proven and no
                # cached aggregate may be served or stored for this request.
                cache_safe = False
            elif _metadata_has_share_row_filter(leaf_meta):
                linked_filter_omitted = True
                logger.info(
                    "[get_super_meta] omitting linked-share-filtered table %s",
                    table,
                )
                continue
            elif (
                (complete_snapshot := _complete_leaf_snapshot(leaf_meta))
                is None
            ):
                # The immutable storage document must be inspected before its
                # policy state is known, so this request bypasses the cache.
                cache_safe = False
            else:
                complete_leaf_snapshots[table] = complete_snapshot

            tables.append(table)
            table_policies[table] = candidate_policies[table]

        if candidate_tables and not tables:
            logger.info(
                "[get_super_meta] no unfiltered metadata targets for role '%s'",
                role_name,
            )
            return None

        leaf_by_table = {
            table: leaf_payloads[index] if index < len(leaf_payloads) else None
            for index, table in enumerate(candidate_tables)
        }

        ttl_s = _super_meta_cache_ttl_s()
        root_version = (root or {}).get("version", 0) if isinstance(root, dict) else 0
        role_id = str(context.role_info.get("role_id", role_name)).casefold()
        cache_key = (
            f"{self.super_table.organization}:{self.super_table.super_name}:"
            f"{role_id}:{context.fingerprint}:{leaf_identity.hexdigest()}"
        )
        if ttl_s > 0 and cache_safe:
            now = time.monotonic()
            with _SUPER_META_CACHE_LOCK:
                cached = _SUPER_META_CACHE.get(cache_key)
            if cached is not None:
                cached_version, expires_at, cached_result = cached
                if cached_version == root_version and now < expires_at:
                    t_end = time.perf_counter()
                    if debug_timings:
                        logger.info(
                            "[timing][get_super_meta] total_ms=%.2f access_ms=%.2f root_ms=%.2f scan_ms=%.2f mget_ms=%.2f tables=%d snapshots=%d snapshots_ms=%.2f max_snapshot_ms=%.2f max_snapshot_table=%s org=%s super=%s role_name=%s cache_hit=1",
                            (t_end - t0) * 1000.0,
                            (t_access - t0) * 1000.0,
                            (t_root - t_access) * 1000.0,
                            (t_scan - t_root) * 1000.0,
                            (t_mget1 - t_mget0) * 1000.0,
                            len(tables),
                            0,
                            0.0,
                            0.0,
                            "",
                            self.super_table.organization,
                            self.super_table.super_name,
                            role_name[:12],
                        )
                    return copy.deepcopy(cached_result)

        simple_table_info = []
        total_files = 0
        total_rows = 0
        total_size = 0

        snapshot_calls = 0
        snapshot_ms_total = 0.0
        max_snapshot_ms = 0.0
        max_snapshot_table = ""

        for idx, table in enumerate(tables):
            try:
                entry = table_policies[table]
                leaf_meta = leaf_by_table.get(table)
                # Never use a partial Redis payload for either the linked-share
                # policy decision or its physical metrics.  A partial cache can
                # legitimately omit fields; only the immutable snapshot selected
                # by the exact leaf is authoritative in that case.
                st_data = complete_leaf_snapshots.get(table)
                pinned_leaf: object = leaf_meta

                if st_data is None:
                    # Fallback: read the current snapshot from storage via SimpleTable.
                    t_st0 = time.perf_counter()
                    st = SimpleTable(
                        self.super_table, table, create_if_missing=False,
                    )
                    st_data, _ = st.get_simple_table_snapshot()
                    pinned_leaf = getattr(st, "_last_snapshot_leaf", None)
                    t_st1 = time.perf_counter()

                    snapshot_calls += 1
                    dt_ms = (t_st1 - t_st0) * 1000.0
                    snapshot_ms_total += dt_ms
                    if dt_ms > max_snapshot_ms:
                        max_snapshot_ms = dt_ms
                        max_snapshot_table = table

                if not isinstance(st_data, dict):
                    raise RuntimeError("Simple table snapshot is invalid")

                if (
                    _metadata_has_share_row_filter(pinned_leaf)
                    or _metadata_has_share_row_filter(st_data)
                ):
                    linked_filter_omitted = True
                    logger.info(
                        "[get_super_meta] omitting linked-share-filtered table %s",
                        table,
                    )
                    continue

                # Calculate table stats
                resources = st_data.get("resources", []) if isinstance(st_data, dict) else []
                table_files = len(resources) if isinstance(resources, list) else 0
                physical_rows = sum(
                    res.get("rows", 0)
                    for res in resources
                    if isinstance(res, dict)
                )
                # Deduct logically-deleted (tombstoned) rows: the deletion-vector
                # model keeps dead rows physically present in the parquet files
                # until threshold compaction, so the resource row sums over-count
                # live rows by the persisted deletion-vector size.  Validate
                # the full v1/v2 discriminated state before using that count;
                # malformed/missing metadata is never interpreted as zero.
                table_rows = _validated_live_row_count(
                    st_data,
                    physical_rows,
                )
                table_size = sum(res.get("file_size", 0) for res in resources if isinstance(res, dict))

                # Count only role-visible columns. Resource-level integer
                # counts cannot be safely adjusted for exclusions, while the
                # pinned logical schema provides exact names.
                visible_schema = _visible_schema(
                    (st_data or {}).get("schema", {}), entry,
                )
                table_cols = len(visible_schema)

                simple_table_info.append({
                    "name": table,
                    "files": table_files,
                    "rows": table_rows,
                    "size": table_size,
                    "cols_count": table_cols,
                    "snapshot_version": (st_data or {}).get("snapshot_version", 0) if isinstance(st_data, dict) else 0,
                    "updated_utc": (st_data or {}).get("last_updated_ms", 0) if isinstance(st_data, dict) else 0,
                })

                total_files += table_files
                total_rows += table_rows
                total_size += table_size

            except (FileNotFoundError, TableNotFoundError) as e:
                logger.debug("Failed to get stats for table %s: %s", table, e)
                continue

        if candidate_tables and not simple_table_info and linked_filter_omitted:
            return None

        result = {
            "super": {
                "name": self.super_table.super_name,
                "files": total_files,
                "rows": total_rows,
                "size": total_size,
                "version": (root or {}).get("version", 0) if isinstance(root, dict) else 0,
                "updated_utc": (root or {}).get("ts", int(datetime.now().timestamp() * 1000)) if isinstance(root, dict) else int(datetime.now().timestamp() * 1000),
                "tables": simple_table_info,
                "meta_path": f"redis://{self.super_table.organization}/{self.super_table.super_name}",
            }
        }

        # Store in burst cache keyed by root version.
        if ttl_s > 0 and cache_safe:
            expires_at = time.monotonic() + ttl_s
            with _SUPER_META_CACHE_LOCK:
                now = time.monotonic()
                expired = [
                    key for key, (_, deadline, _) in _SUPER_META_CACHE.items()
                    if deadline <= now
                ]
                for key in expired:
                    _SUPER_META_CACHE.pop(key, None)
                while len(_SUPER_META_CACHE) >= _SUPER_META_CACHE_MAX_ENTRIES:
                    _SUPER_META_CACHE.pop(next(iter(_SUPER_META_CACHE)))
                _SUPER_META_CACHE[cache_key] = (
                    root_version, expires_at, copy.deepcopy(result),
                )

        t_end = time.perf_counter()
        if debug_timings:
            logger.info(
                "[timing][get_super_meta] total_ms=%.2f access_ms=%.2f root_ms=%.2f scan_ms=%.2f mget_ms=%.2f tables=%d snapshots=%d snapshots_ms=%.2f max_snapshot_ms=%.2f max_snapshot_table=%s org=%s super=%s role_name=%s cache_hit=0",
                (t_end - t0) * 1000.0,
                (t_access - t0) * 1000.0,
                (t_root - t_access) * 1000.0,
                (t_scan - t_root) * 1000.0,
                (t_mget1 - t_mget0) * 1000.0,
                len(tables),
                snapshot_calls,
                snapshot_ms_total,
                max_snapshot_ms,
                max_snapshot_table,
                self.super_table.organization,
                self.super_table.super_name,
                role_name[:12],
            )

        return result




def list_supers(organization: str, role_name: str) -> List[str]:
    """
    Searches the organization's directory for subdirectories that contain a
    "super" folder and a "_super.json" file. Uses the storage interface's
    get_directory_structure() for portability.

    Filters results to only include supers where *role_name* has META access.
    """
    result = set()
    pattern = RK.meta_root_pattern_for_org(organization)
    catalog = RedisCatalog()

    items = _get_redis_items(pattern)
    for item in items:
        parsed = RK.parse_lake_key(item)
        if parsed is None or parsed[0] != organization:
            raise RuntimeError("Redis root scan returned an invalid catalog key")
        _, super_name = parsed
        if item != RK.meta_root(organization, super_name):
            raise RuntimeError("Redis root scan returned an invalid catalog key")
        if catalog.get_root(organization, super_name) is None:
            raise RuntimeError(
                f"Catalog root disappeared during discovery: "
                f"{organization}/{super_name}"
            )
        # A supertable name is a synthetic aggregate relation, not a physical
        # table policy target. Inclusion-only roles therefore discover it via
        # at least one authorized child rather than a nonexistent parent entry.
        for leaf_item in catalog.scan_leaf_items(
            organization, super_name, count=1000,
        ):
            if not isinstance(leaf_item, dict):
                raise RuntimeError("Catalog leaf scan returned an invalid item")
            table_name = leaf_item.get("simple")
            if not isinstance(table_name, str) or not table_name:
                raise RuntimeError(
                    "Catalog leaf scan returned an invalid table name"
                )
            catalog.check_deletion_intent_absent(
                organization, super_name, simple=table_name,
            )
            if (
                table_name.startswith("__") and table_name.endswith("__")
            ):
                continue
            try:
                check_meta_access(
                    super_name=super_name,
                    organization=organization,
                    role_name=role_name,
                    table_name=table_name,
                )
                result.add(super_name)
                break
            except PermissionError:
                continue

    return sorted(result)


def list_tables(organization: str, super_name: str, role_name: str) -> List[str]:
    """
    Searches the organization's directory for subdirectories that contain a
    "super" folder and a "_super.json" file. Uses the storage interface's
    get_directory_structure() for portability.

    Filters results to only include tables where *role_name* has META access.
    """
    result = set()
    catalog = RedisCatalog()
    for item in catalog.scan_leaf_items(
        organization, super_name, count=1000,
    ):
        if not isinstance(item, dict):
            raise RuntimeError("Catalog leaf scan returned an invalid item")
        table_name = item.get("simple")
        if not isinstance(table_name, str) or not table_name:
            raise RuntimeError("Catalog leaf scan returned an invalid table name")
        catalog.check_deletion_intent_absent(
            organization, super_name, simple=table_name,
        )
        if table_name.startswith("__") and table_name.endswith("__"):
            continue
        try:
            check_meta_access(
                super_name=super_name,
                organization=organization,
                role_name=role_name,
                table_name=table_name,
            )
            result.add(table_name)
        except PermissionError:
            continue

    return sorted(result)
