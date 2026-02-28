import os
import logging
import json
import time
import threading
from datetime import datetime
from typing import List, Optional, Dict, Any, Set, Tuple

from supertable.rbac.access_control import check_meta_access
from supertable.redis_catalog import RedisCatalog

from supertable.super_table import SuperTable
from supertable.simple_table import SimpleTable

logger = logging.getLogger(__name__)


# Small in-process cache to de-duplicate bursty reflection calls.
_SUPER_META_CACHE: Dict[str, Tuple[int, float, Dict[str, Any]]] = {}
_SUPER_META_CACHE_LOCK = threading.Lock()

def _super_meta_cache_ttl_s() -> float:
    val = (os.getenv("SUPERTABLE_SUPER_META_CACHE_TTL_S", "") or "").strip()
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
        # Create a SuperTable object (which internally sets up the storage backend).
        self.super_table = SuperTable(super_name=super_name, organization=organization)
        self.catalog = RedisCatalog()


    def _get_all_tables(self) -> List[str]:
        """
        Get all tables for this super table by scanning Redis keys.
        """
        try:
            # Pattern to match all leaf pointers for this organization/super
            pattern = f"supertable:{self.super_table.organization}:{self.super_table.super_name}:meta:leaf:*"

            tables = []
            seen = set()
            cursor = 0
            while True:
                cursor, keys = self.catalog.r.scan(cursor=cursor, match=pattern, count=1000)
                for key in keys:
                    # Handle both bytes and string keys
                    if isinstance(key, bytes):
                        key_str = key.decode('utf-8')
                    else:
                        key_str = str(key)

                    # Extract table name from key: supertable:org:super:meta:leaf:table_name
                    table_name = key_str.split(':')[-1]
                    if table_name and table_name not in seen:
                        seen.add(table_name)
                        tables.append(table_name)
                if cursor == 0:
                    break
            return tables
        except Exception as e:
            logger.error(f"Error getting tables from Redis: {e}")
            return []

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
            try:
                check_meta_access(
                    super_name=self.super_table.super_name,
                    organization=self.super_table.organization,
                    role_name=role_name,
                    table_name=table_name,
                )
            except PermissionError as e:
                logger.warning(
                    "[get_table_schema] Access denied for user '%s' on table '%s': %s",
                    role_name, table_name, str(e)
                )
                return None

            schema_items: Set[Tuple[str, Any]] = set()

            if table_name == self.super_table.super_name:
                # Aggregate schema across all simple tables (prefer Redis leaf payload; fallback to storage).
                tables = self._get_all_tables()
                leaf_keys: List[str] = [
                    f"supertable:{self.super_table.organization}:{self.super_table.super_name}:meta:leaf:{t}"
                    for t in tables
                ]
                try:
                    raws = self.catalog.r.mget(leaf_keys) if leaf_keys else []
                except Exception:
                    raws = []

                for t, raw in zip(tables, raws):
                    try:
                        leaf_meta = _try_parse_leaf_meta(raw)
                        st_data = _leaf_to_snapshot_like(leaf_meta or {}) if isinstance(leaf_meta, dict) else None
                        if st_data is None:
                            simple_table = SimpleTable(self.super_table, t)
                            st_data, _ = simple_table.get_simple_table_snapshot()

                        schema = _schema_to_dict((st_data or {}).get("schema", {}))
                        for key, value in schema.items():
                            schema_items.add((key, value))
                    except (FileNotFoundError, KeyError) as e:
                        logger.debug("Failed to read schema for table %s: %s", t, e)
                        continue
            else:
                # Single table (prefer Redis leaf payload; fallback to storage).
                try:
                    raw = self.catalog.r.get(
                        f"supertable:{self.super_table.organization}:{self.super_table.super_name}:meta:leaf:{table_name}"
                    )
                    leaf_meta = _try_parse_leaf_meta(raw)
                    st_data = _leaf_to_snapshot_like(leaf_meta or {}) if isinstance(leaf_meta, dict) else None

                    if st_data is None:
                        simple_table = SimpleTable(self.super_table, table_name)
                        st_data, _ = simple_table.get_simple_table_snapshot()

                    schema = _schema_to_dict((st_data or {}).get("schema", {}))
                    for key, value in schema.items():
                        schema_items.add((key, value))
                except (FileNotFoundError, KeyError) as e:
                    logger.debug("Failed to read schema for table %s: %s", table_name, e)
                    return [{}]

            distinct_schema = dict(sorted(schema_items))
            return [distinct_schema]

    def collect_simple_table_schema(self, schemas: set, table_name: str, role_name: str) -> None:
        try:
            check_meta_access(
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
            simple_table = SimpleTable(self.super_table, table_name)
            simple_table_data, _ = simple_table.get_simple_table_snapshot()
        except FileNotFoundError:
            logger.debug("Simple table snapshot missing for %s", table_name)
            return

        schema = simple_table_data.get("schema", {}) or {}
        schema_tuple = tuple(sorted(schema.items()))
        schemas.add(schema_tuple)

    def get_table_stats(self, table_name: str, role_name: str) -> List[Dict[str, Any]]:
        try:
            check_meta_access(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                role_name=role_name,
                table_name=table_name,
            )
        except PermissionError as e:
            logger.warning(
                "[get_table_stats] Access denied for user '%s' on table '%s': %s",
                role_name, table_name, str(e)
            )
            return []

        keys_to_remove = {"previous_snapshot", "schema", "location"}
        stats: List[Dict[str, Any]] = []

        if table_name == self.super_table.super_name:
            # Get all tables and their stats
            tables = self._get_all_tables()
            for table in tables:
                try:
                    st = SimpleTable(self.super_table, table)
                    st_data, _ = st.get_simple_table_snapshot()
                    stats.append(_prune_dict(st_data, keys_to_remove))
                except (FileNotFoundError, KeyError):
                    logger.debug("Simple table snapshot missing for %s", table)
                    continue
        else:
            # Single table
            try:
                st = SimpleTable(self.super_table, table_name)
                st_data, _ = st.get_simple_table_snapshot()
                stats.append(_prune_dict(st_data, keys_to_remove))
            except (FileNotFoundError, KeyError):
                logger.debug("Simple table snapshot missing for %s", table_name)
                return []

        return stats

    def get_super_meta(self, role_name: str) -> Optional[Dict[str, Any]]:

        debug_timings = (os.getenv("SUPERTABLE_DEBUG_TIMINGS", "") or "").lower() in ("1", "true", "yes", "on")
        t0 = time.perf_counter()

        try:
            # Checking meta access for the super table itself
            check_meta_access(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                role_name=role_name,
                table_name=self.super_table.super_name,
            )
        except PermissionError as e:
            logger.warning(
                "[get_super_meta] Access denied for user '%s' on super '%s': %s",
                role_name, self.super_table.super_name, str(e)
            )
            return None

        t_access = time.perf_counter()

        root = self.catalog.get_root(org=self.super_table.organization, sup=self.super_table.super_name)
        t_root = time.perf_counter()

        ttl_s = _super_meta_cache_ttl_s()
        root_version = (root or {}).get("version", 0) if isinstance(root, dict) else 0
        cache_key = f"{self.super_table.organization}:{self.super_table.super_name}:{role_name}"
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
                    return cached_result

        # Get all tables from Redis
        tables = self._get_all_tables()
        t_scan = time.perf_counter()

        # Best-effort bulk fetch leaf metadata in one Redis roundtrip.
        leaf_payloads: List[Optional[Dict[str, Any]]] = []
        leaf_keys: List[str] = [
            f"supertable:{self.super_table.organization}:{self.super_table.super_name}:meta:leaf:{t}"
            for t in tables
        ]
        t_mget0 = time.perf_counter()
        try:
            raws = self.catalog.r.mget(leaf_keys) if leaf_keys else []
            leaf_payloads = [_try_parse_leaf_meta(r) for r in raws]
        except Exception:
            leaf_payloads = [None for _ in tables]
        t_mget1 = time.perf_counter()

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
                leaf_meta = leaf_payloads[idx] if idx < len(leaf_payloads) else None
                st_data = _leaf_to_snapshot_like(leaf_meta or {}) if isinstance(leaf_meta, dict) else None

                if st_data is None:
                    # Fallback: read the current snapshot from storage via SimpleTable.
                    t_st0 = time.perf_counter()
                    st = SimpleTable(self.super_table, table)
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
                table_size = sum(res.get("file_size", 0) for res in resources if isinstance(res, dict))

                simple_table_info.append({
                    "name": table,
                    "files": table_files,
                    "rows": table_rows,
                    "size": table_size,
                    "updated_utc": (st_data or {}).get("last_updated_ms", 0) if isinstance(st_data, dict) else 0,
                })

                total_files += table_files
                total_rows += table_rows
                total_size += table_size

            except (FileNotFoundError, KeyError) as e:
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
                _SUPER_META_CACHE[cache_key] = (root_version, expires_at, result)

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




def list_supers(organization: str) -> List[str]:
    """
    Searches the organization's directory for subdirectories that contain a
    "super" folder and a "_super.json" file. Uses the storage interface's
    get_directory_structure() for portability.
    """
    result = []
    pattern = f"supertable:{organization}:*:meta:root"

    items = _get_redis_items(pattern)
    for item in items:
        super_name = item.split(':')[2]
        result.append(super_name)

    return sorted(result)


def list_tables(organization: str, super_name: str) -> List[str]:
    """
    Searches the organization's directory for subdirectories that contain a
    "super" folder and a "_super.json" file. Uses the storage interface's
    get_directory_structure() for portability.
    """
    result = []
    pattern = f"supertable:{organization}:{super_name}:meta:leaf:*"

    items = _get_redis_items(pattern)
    for item in items:
        table_name = item.split(':')[-1]
        result.append(table_name)

    return sorted(result)