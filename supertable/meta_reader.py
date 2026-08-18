import os
import copy

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


def _sanitize_snapshot_stats(snapshot: Dict[str, Any], entry: dict) -> Dict[str, Any]:
    """Remove column-policy leaks while retaining useful table/file metrics."""
    result = _prune_dict(
        snapshot,
        {"previous_snapshot", "schema", "schemaString", "location"},
    )
    schema = _schema_to_dict(snapshot.get("schema", {}))
    visible_schema = _visible_schema(schema, entry)
    visible_folded = {name.casefold() for name in visible_schema}

    resources = result.get("resources")
    if isinstance(resources, list):
        cleaned_resources = []
        for resource in resources:
            if not isinstance(resource, dict):
                continue
            cleaned = dict(resource)
            raw_columns = cleaned.get("columns")
            if isinstance(raw_columns, list):
                kept = []
                for item in raw_columns:
                    name = item.get("name") if isinstance(item, dict) else item
                    if isinstance(name, str) and name.casefold() in visible_folded:
                        kept.append(item)
                cleaned["columns"] = kept
            elif "columns" in cleaned:
                # A numeric count cannot be reduced without revealing the
                # hidden-column count. Omit it instead of inventing a value.
                cleaned.pop("columns", None)
            for field in ("column_max_value_bytes", "integer_domain_bounds"):
                value = cleaned.get(field)
                if isinstance(value, dict):
                    cleaned[field] = {
                        name: payload for name, payload in value.items()
                        if str(name).casefold() in visible_folded
                    }
            cleaned_resources.append(cleaned)
        result["resources"] = cleaned_resources

    for field in ("column_max_value_bytes", "integer_domain_bounds"):
        value = result.get(field)
        if isinstance(value, dict):
            result[field] = {
                name: payload for name, payload in value.items()
                if str(name).casefold() in visible_folded
            }
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
    try:
        items = []
        cursor = 0
        while True:
            cursor, keys = catalog.r.scan(cursor=cursor, match=pattern, count=1000)
            for key in keys:
                # Handle both bytes and string keys
                if isinstance(key, bytes):
                    key_str = key.decode('utf-8')
                else:
                    key_str = str(key)

                items.append(key_str)
            if cursor == 0:
                break
        return items
    except Exception as e:
        logger.error(f"Error getting tables from Redis: {e}")
        return []




def _try_parse_leaf_meta(raw: Any) -> Optional[Dict[str, Any]]:
    """Best-effort JSON decode for Redis leaf values (bytes/str)."""
    if raw is None:
        return None
    try:
        if isinstance(raw, (bytes, bytearray)):
            raw_s = raw.decode("utf-8")
        else:
            raw_s = str(raw)
        raw_s = raw_s.strip()
        if not raw_s:
            return None
        # Most leaf values are JSON payloads.
        return json.loads(raw_s)
    except Exception:
        return None


def _leaf_to_snapshot_like(meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Return a dict that looks like SimpleTable snapshot data if the Redis leaf already stores it.
    This is an optimization to avoid hitting object storage for reflection endpoints.
    """
    if not isinstance(meta, dict):
        return None
    if isinstance(meta.get("resources"), list):
        return meta

    payload = meta.get("payload")
    if isinstance(payload, dict):
        if isinstance(payload.get("resources"), list):
            return payload
        snapshot = payload.get("snapshot")
        if isinstance(snapshot, dict) and isinstance(snapshot.get("resources"), list):
            return snapshot
    data = meta.get("data")
    if isinstance(data, dict) and isinstance(data.get("resources"), list):
        return data
    snapshot = meta.get("snapshot")
    if isinstance(snapshot, dict) and isinstance(snapshot.get("resources"), list):
        return snapshot
    return None


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
        """Get all tables for this super table via catalog (replica-aware)."""
        try:
            tables = []
            seen = set()
            for key in self.catalog.scan_leaf_keys(
                self.super_table.organization,
                self.super_table.super_name,
                count=1000,
            ):
                key_str = key if isinstance(key, str) else key.decode('utf-8')
                table_name = key_str.rsplit("meta:leaf:doc:", 1)[-1]
                if (
                    table_name
                    and not (
                        table_name.startswith("__")
                        and table_name.endswith("__")
                    )
                    and table_name not in seen
                ):
                    seen.add(table_name)
                    tables.append(table_name)
            return tables
        except Exception as e:
            logger.error(f"Error getting tables from Redis: {e}")
            return []

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
            except Exception as e:
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
        try:
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
        except Exception:
            raws = [None] * len(targets)

        for index, target in enumerate(targets):
            entry = table_policies[target]
            try:
                raw = raws[index] if index < len(raws) else None
                leaf_meta = _try_parse_leaf_meta(raw)
                st_data = (
                    _leaf_to_snapshot_like(leaf_meta or {})
                    if isinstance(leaf_meta, dict) else None
                )
                if st_data is None:
                    simple_table = SimpleTable(
                        self.super_table, target, create_if_missing=False,
                    )
                    st_data, _ = simple_table.get_simple_table_snapshot()
                schema = _visible_schema((st_data or {}).get("schema", {}), entry)
                schema_items.update(schema.items())
            except (FileNotFoundError, KeyError, TableNotFoundError) as e:
                logger.debug("Failed to read schema for table %s: %s", target, e)
                continue

        return [dict(sorted(schema_items))]

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
            try:
                st = SimpleTable(
                    self.super_table, table, create_if_missing=False,
                )
                st_data, _ = st.get_simple_table_snapshot()
                stats.append(_sanitize_snapshot_stats(st_data, entry))
            except (FileNotFoundError, KeyError, TableNotFoundError):
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

        ttl_s = _super_meta_cache_ttl_s()
        root_version = (root or {}).get("version", 0) if isinstance(root, dict) else 0
        role_id = str(context.role_info.get("role_id", role_name)).casefold()
        cache_key = (
            f"{self.super_table.organization}:{self.super_table.super_name}:"
            f"{role_id}:{context.fingerprint}"
        )
        if ttl_s > 0:
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
                            0.0,
                            0.0,
                            0,
                            0,
                            0.0,
                            0.0,
                            "",
                            self.super_table.organization,
                            self.super_table.super_name,
                            role_name[:12],
                        )
                    return copy.deepcopy(cached_result)

        # Get all tables from Redis
        all_tables = [table for table, _entry in authorized]
        tables = list(all_tables)
        table_policies: Dict[str, dict] = dict(authorized)
        t_scan = time.perf_counter()

        # Best-effort bulk fetch leaf metadata in one Redis roundtrip.
        leaf_payloads: List[Optional[Dict[str, Any]]] = []
        leaf_keys: List[str] = [
            RK.meta_leaf(self.super_table.organization, self.super_table.super_name, t)
            for t in all_tables
        ]
        t_mget0 = time.perf_counter()
        try:
            raws = self.catalog.r.mget(leaf_keys) if leaf_keys else []
            leaf_payloads = [_try_parse_leaf_meta(r) for r in raws]
        except Exception:
            leaf_payloads = [None for _ in all_tables]
        t_mget1 = time.perf_counter()
        leaf_by_table = {
            table: leaf_payloads[index] if index < len(leaf_payloads) else None
            for index, table in enumerate(all_tables)
        }

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
                st_data = _leaf_to_snapshot_like(leaf_meta or {}) if isinstance(leaf_meta, dict) else None

                if st_data is None:
                    # Fallback: read the current snapshot from storage via SimpleTable.
                    t_st0 = time.perf_counter()
                    st = SimpleTable(
                        self.super_table, table, create_if_missing=False,
                    )
                    st_data, _ = st.get_simple_table_snapshot()
                    t_st1 = time.perf_counter()

                    snapshot_calls += 1
                    dt_ms = (t_st1 - t_st0) * 1000.0
                    snapshot_ms_total += dt_ms
                    if dt_ms > max_snapshot_ms:
                        max_snapshot_ms = dt_ms
                        max_snapshot_table = table

                # Calculate table stats
                resources = st_data.get("resources", []) if isinstance(st_data, dict) else []
                table_files = len(resources) if isinstance(resources, list) else 0
                table_rows = sum(res.get("rows", 0) for res in resources if isinstance(res, dict))
                # Deduct logically-deleted (tombstoned) rows: the deletion-vector
                # model keeps dead rows physically present in the parquet files
                # until threshold compaction, so the resource row sums over-count
                # live rows by the persisted deletion-vector size.
                tombstone_rows = st_data.get("tombstone_rows", 0) if isinstance(st_data, dict) else 0
                table_rows = max(0, table_rows - int(tombstone_rows or 0))
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

            except (FileNotFoundError, KeyError, TableNotFoundError) as e:
                logger.debug("Failed to get stats for table %s: %s", table, e)
                continue

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
        if ttl_s > 0:
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
    result = []
    pattern = RK.meta_root_pattern_for_org(organization)

    items = _get_redis_items(pattern)
    for item in items:
        parsed = RK.parse_lake_key(item)
        if parsed is None:
            continue
        _, super_name = parsed
        # A supertable name is a synthetic aggregate relation, not a physical
        # table policy target. Inclusion-only roles therefore discover it via
        # at least one authorized child rather than a nonexistent parent entry.
        for leaf_item in _get_redis_items(
            RK.meta_leaf_pattern(organization, super_name)
        ):
            table_name = leaf_item.rsplit("meta:leaf:doc:", 1)[-1]
            if not table_name or (
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
                result.append(super_name)
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
    result = []
    pattern = RK.meta_leaf_pattern(organization, super_name)

    items = _get_redis_items(pattern)
    for item in items:
        table_name = item.split(':')[-1]
        if table_name.startswith("__") and table_name.endswith("__"):
            continue
        try:
            check_meta_access(
                super_name=super_name,
                organization=organization,
                role_name=role_name,
                table_name=table_name,
            )
            result.append(table_name)
        except PermissionError:
            continue

    return sorted(result)
