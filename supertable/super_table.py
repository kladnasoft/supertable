# supertable/super_table.py

from __future__ import annotations

import os
import copy
import json
import time
import uuid
from typing import Any, Callable, Dict, Optional

from supertable.config.defaults import logger
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.user_manager import UserManager
from supertable.errors import SuperTableNotFoundError
from supertable.storage.storage_factory import get_storage
from supertable.storage.storage_interface import StorageInterface
from supertable.redis_catalog import ReadOnlyCatalogError, RedisCatalog
from supertable.redis_keys import is_reserved_super_name, RESERVED_SUPER_NAMES
from supertable.rbac.access_control import resolve_role_access_context
from supertable.rbac.permissions import Permission, RoleType
from supertable.utils.helper import generate_filename


_LEGACY_METADATA_MIGRATION_VERSION = 2
_MAX_MIGRATION_SNAPSHOT_BYTES = 8 * 1024 * 1024
_MAX_MIGRATION_SCHEMA_BYTES = 1024 * 1024
_MAX_MIGRATION_TABLES = 10_000
_MAX_MIGRATION_LEAF_INDEX_BYTES = 16 * 1024 * 1024
_MAX_MIGRATION_SCAN_CALLS = 1_000
_MAX_MIGRATION_COLUMN_CHUNKS = 100_000
_MAX_MIGRATION_STATS_ROWS = 100_000
_MAX_MIGRATION_ARRAY_PAGE_BYTES = 8 * 1024 * 1024
_MAX_MIGRATION_PAGE_HEADER_BYTES = 64 * 1024
_MAX_MIGRATION_PARQUET_PAGES = 10_000_000
_MAX_MIGRATION_ARRAY_WORKER_RSS_BYTES = 256 * 1024 * 1024
_MAX_MIGRATION_ARRAY_WORKER_HEADROOM_BYTES = 1024 * 1024 * 1024
_MAX_MIGRATION_ARRAY_WORKER_STALL_SECONDS = 10 * 60
# A marked-table rerun validates the immutable stats artifact through the
# runtime's 64 MiB bounded diagnostic reader. Never publish a migration frame
# whose working-set envelope that same recovery path cannot reopen.
_MAX_MIGRATION_STATS_DECODED_BYTES = 64 * 1024 * 1024
_MAX_REDIS_ROOT_VERSION = (1 << 53) - 1


class NamespaceCleanupPostCommitError(RuntimeError):
    """The namespace deletion committed but an application cleanup failed.

    ``core_committed`` and ``core_result`` deliberately mirror the SDK's
    other post-commit exceptions so HTTP/control-plane adapters can prohibit a
    destructive retry while directing operators to reconcile the auxiliary
    state.  Backend exception chaining is deliberately suppressed because
    formatted tracebacks are an externally visible diagnostic surface.
    """

    def __init__(self, intent_id: str) -> None:
        self.intent_id = str(intent_id)
        self.core_committed = True
        self.core_result = self.intent_id
        self.subsystem = "platform_cleanup"
        super().__init__(
            "SuperTable deletion committed, but fenced platform cleanup failed"
        )


class SuperTable:
    """
    Minimal coordination object:
      - Ensures storage backend is available
      - Ensures Redis meta:root exists (no file-based meta)
      - Exposes helper to read heavy simple-table snapshots from MinIO/local via StorageInterface

    Reserved supertable names (any underscore-wrapped name like
    ``_foo_`` — matched by ``redis_keys.SENTINEL_RE``) are rejected
    up-front. Lakes live under ``supertable:{org}:lakes:{sup}``, so
    structurally they could not collide with the org-level ``system:``
    namespace anyway — the sentinel-pattern reservation is defence in
    depth against future system labels.

    Args:
        super_name: Name of the supertable (organization-scoped).
        organization: Organization (tenant) name.
        create_if_missing: When True (default), bootstrap the supertable
            (storage mkdir, Redis ``meta:root``, RBAC scaffolding) if it
            does not exist. When False, raise
            ``SuperTableNotFoundError`` instead. Read-side callers
            (``DataReader``, ``MetaReader``, ``DataEstimator``) pass
            ``False`` so a missing supertable surfaces as an error
            instead of being silently materialized as a side effect of
            constructing the Python object.
    """

    def __init__(
        self,
        super_name: str,
        organization: str,
        *,
        create_if_missing: bool = True,
    ):
        if is_reserved_super_name(super_name):
            raise ValueError(
                f"SuperTable name {super_name!r} is reserved and cannot be created. "
                f"Reserved names: {sorted(RESERVED_SUPER_NAMES)}"
            )

        self.identity = "super"
        self.super_name = super_name
        self.organization = organization

        # Storage for heavy JSON + parquet
        self.storage: StorageInterface = get_storage()

        # Redis catalog for meta & locking
        self.catalog = RedisCatalog()

        deletion_guard = getattr(
            type(self.catalog), "check_deletion_intent_absent", None,
        )
        if callable(deletion_guard):
            self.catalog.check_deletion_intent_absent(
                self.organization,
                self.super_name,
            )

        # Directories for data layout (still used for heavy JSON & data files)
        self.super_dir = os.path.join(self.organization, self.super_name, self.identity)

        # Fast path: if meta:root exists, don't touch storage
        if self.catalog.root_exists(self.organization, self.super_name):
            return

        # Read-only opt-out: refuse to bootstrap as a side effect. This
        # is the guarantee that lets ``DataReader`` / ``MetaReader`` open
        # a session against a missing name and get a clean, named error
        # back instead of silently creating an empty supertable.
        if not create_if_missing:
            raise SuperTableNotFoundError(organization, super_name)

        self.init_super_table()

        # Initialize RBAC scaffolding
        RoleManager(super_name=self.super_name, organization=self.organization)
        UserManager(super_name=self.super_name, organization=self.organization)

    # ------------------------------------------------------------------ init
    def init_super_table(self) -> None:
        """
        Initialize super table:
          * If Redis meta:root already exists -> skip any folder checks/creations.
          * Otherwise, create the base folder (best-effort) and bootstrap Redis meta:root.
        """

        token = self.catalog.acquire_namespace_lock(
            self.organization, self.super_name, ttl_s=30, timeout_s=60,
        )
        if not token:
            raise TimeoutError("Could not acquire the namespace creation lock")
        try:
            # Check before either fast-path return: stale root metadata behind
            # a terminal tombstone must never reopen a deleted namespace.
            self.catalog.check_initialization_allowed(
                self.organization,
                self.super_name,
                namespace_token=token,
            )
            if self.catalog.root_exists(self.organization, self.super_name):
                return
            # The structural lock is acquired before the first storage write,
            # so an initializer paused here cannot write a directory marker
            # after a concurrent verified namespace deletion.
            try:
                self.storage.makedirs(self.super_dir)
            except Exception:
                # Object storage may no-op; that's fine
                pass

            self.catalog.ensure_root(
                self.organization,
                self.super_name,
                namespace_token=token,
            )
        finally:
            self.catalog.release_namespace_lock(
                self.organization, self.super_name, token,
            )

    # ------------------------------------------------------------------ heavy JSON read
    def read_simple_table_snapshot(self, simple_table_path: str) -> Dict[str, Any]:
        """
        Read the **heavy** simple-table snapshot JSON from storage (MinIO/local).
        """
        if not simple_table_path or not self.storage.exists(simple_table_path):
            raise FileNotFoundError("Simple table snapshot was not found")
        if self.storage.size(simple_table_path) == 0:
            raise ValueError("Simple table snapshot is empty")
        return self.storage.read_json(simple_table_path)

    def migrate_legacy_metadata(
        self,
        *,
        confirm_system_offline: bool = False,
        expected_tables: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Verify and upgrade legacy table snapshots in this supertable.

        The migration is deliberately explicit: every referenced data and
        tombstone object must be readable before a successor snapshot is
        published.  Missing statistics are rebuilt from Parquet footers; no
        rows or deletion-vector entries are invented.  v2.4 processes do not
        understand the current namespace fence, so the operator must stop all
        system traffic and processes before making this exact confirmation.
        """
        if confirm_system_offline is not True:
            raise ValueError(
                "Offline migration requires confirm_system_offline=True"
            )
        expected_inventory: Optional[frozenset[str]] = None
        if expected_tables is not None:
            if isinstance(expected_tables, (str, bytes, dict)):
                raise ValueError(
                    "expected_tables must be a non-empty collection of names"
                )
            try:
                expected_items = list(expected_tables)
            except TypeError:
                raise ValueError(
                    "expected_tables must be a non-empty collection of names"
                ) from None
            if (
                not expected_items
                or len(expected_items) > _MAX_MIGRATION_TABLES
                or any(
                    not isinstance(name, str) or not name
                    for name in expected_items
                )
                or len(set(expected_items)) != len(expected_items)
            ):
                raise ValueError(
                    "expected_tables must contain unique non-empty names"
                )
            expected_inventory = frozenset(expected_items)
        token = self.catalog.acquire_namespace_lock(
            self.organization, self.super_name, ttl_s=60, timeout_s=60,
        )
        if not token:
            raise TimeoutError("Could not acquire namespace migration lock")
        migrated: list[str] = []
        try:
            root = self.catalog.get_root(self.organization, self.super_name)
            if not isinstance(root, dict):
                raise FileNotFoundError("SuperTable catalog root was not found")
            root_version = root.get("version")
            if (
                type(root_version) is not int
                or root_version < 0
                or root_version > _MAX_REDIS_ROOT_VERSION
            ):
                raise ValueError(
                    "SuperTable catalog root has an invalid version"
                )
            if root.get("read_only") is True or root.get("clone_type") in {
                "readonly", "replica",
            }:
                raise ReadOnlyCatalogError(
                    "Legacy metadata migration requires a writable namespace"
                )
            if self.catalog.get_mirrors(self.organization, self.super_name):
                raise RuntimeError(
                    "Legacy metadata migration for mirror-enabled tables "
                    "requires mirror reconciliation support"
                )
            # Redis SCAN may return the same key more than once.  Retain only
            # the identity needed for the later compare-and-swap preflight,
            # and force catalog batches to one item so a collection of large
            # cached payloads cannot create an unbounded transient allocation.
            leaves: list[Dict[str, Any]] = []
            seen_leaves: dict[str, tuple[int, str]] = {}
            retained_leaf_bytes = 0
            for scanned_leaf in self.catalog.scan_leaf_items(
                self.organization,
                self.super_name,
                count=1000,
                batch_size=1,
                max_scan_calls=_MAX_MIGRATION_SCAN_CALLS,
            ):
                if not isinstance(scanned_leaf, dict):
                    raise ValueError("Migration catalog contains an invalid leaf")
                simple = scanned_leaf.get("simple")
                path = scanned_leaf.get("path")
                version = scanned_leaf.get("version")
                if (
                    not isinstance(simple, str)
                    or not simple
                    or not isinstance(path, str)
                    or not path
                    or type(version) is not int
                    or version < 0
                ):
                    raise ValueError("Migration catalog contains an invalid leaf")
                identity = (version, path)
                previous_identity = seen_leaves.get(simple)
                if previous_identity is not None:
                    if previous_identity != identity:
                        raise RuntimeError(
                            f"Table {simple} changed during migration discovery"
                        )
                    continue
                try:
                    retained_leaf_bytes += (
                        len(simple.encode("utf-8"))
                        + len(path.encode("utf-8"))
                        + 32
                    )
                except UnicodeEncodeError:
                    raise ValueError(
                        "Migration catalog contains an invalid leaf"
                    ) from None
                if (
                    len(leaves) >= _MAX_MIGRATION_TABLES
                    or retained_leaf_bytes > _MAX_MIGRATION_LEAF_INDEX_BYTES
                ):
                    raise ValueError(
                        "Migration catalog exceeds its table-index safety bound"
                    )
                seen_leaves[simple] = identity
                leaves.append({
                    "simple": simple,
                    "path": path,
                    "version": version,
                })
            if not leaves:
                raise RuntimeError(
                    "Offline migration namespace contains no tables; verify "
                    "the production organization and supertable name"
                )
            if (
                expected_inventory is not None
                and frozenset(seen_leaves) != expected_inventory
            ):
                raise RuntimeError(
                    "Offline migration table inventory does not match the "
                    "operator-approved production inventory"
                )
            # Pass one performs all source reads and semantic validation but
            # publishes nothing.  Only after every table passes do we repeat
            # the pinned validation and create successors one table at a time.
            # This keeps a corrupt late table from leaving the namespace half
            # migrated on the first production attempt.
            planned_migrations = 0
            for preflight_only in (True, False):
                for scanned_leaf in leaves:
                    simple = scanned_leaf["simple"]
                    simple_token = self.catalog.acquire_simple_lock(
                        self.organization,
                        self.super_name,
                        simple,
                        ttl_s=60,
                        timeout_s=60,
                    )
                    if not simple_token:
                        raise TimeoutError(
                            f"Could not acquire migration lock for table {simple}"
                        )
                    try:
                        mutation = self.catalog.begin_table_mutation(
                            self.organization,
                            self.super_name,
                            simple,
                            lock_token=simple_token,
                            namespace_token=token,
                        )
                        pinned_mirrors = mutation.get("mirrors")
                        if not isinstance(pinned_mirrors, list):
                            raise RuntimeError("Mirror configuration is invalid")
                        if pinned_mirrors:
                            raise RuntimeError(
                                "Legacy metadata migration for mirror-enabled "
                                "tables requires mirror reconciliation support"
                            )
                        leaf = mutation.get("leaf")
                        if not isinstance(leaf, dict):
                            raise FileNotFoundError(
                                f"Table {simple} disappeared during migration"
                            )
                        if (
                            leaf.get("path") != scanned_leaf.get("path")
                            or leaf.get("version") != scanned_leaf.get("version")
                        ):
                            raise RuntimeError(
                                f"Table {simple} changed during migration"
                            )
                        was_migrated = self._migrate_legacy_leaf(
                            simple=simple,
                            leaf=leaf,
                            namespace_token=token,
                            simple_token=simple_token,
                            mirror_pin=mutation.get("mirror_pin"),
                            preflight_only=preflight_only,
                        )
                        if preflight_only and was_migrated:
                            planned_migrations += 1
                        if not preflight_only and was_migrated:
                            migrated.append(simple)
                        if preflight_only or not was_migrated:
                            # No commit occurred, so fence the completion of
                            # the potentially long validation against the same
                            # leaf and mirror generation pinned at its start.
                            verified = self.catalog.begin_table_mutation(
                                self.organization,
                                self.super_name,
                                simple,
                                lock_token=simple_token,
                                namespace_token=token,
                            )
                            if (
                                verified.get("leaf") != leaf
                                or verified.get("mirrors") != pinned_mirrors
                                or verified.get("mirror_pin")
                                != mutation.get("mirror_pin")
                            ):
                                raise RuntimeError(
                                    f"Table {simple} changed during migration "
                                    "validation"
                                )
                    finally:
                        self.catalog.release_simple_lock(
                            self.organization,
                            self.super_name,
                            simple,
                            simple_token,
                        )
                if preflight_only:
                    verified_root = self.catalog.get_root(
                        self.organization,
                        self.super_name,
                    )
                    if (
                        not isinstance(verified_root, dict)
                        or verified_root.get("version") != root_version
                    ):
                        raise RuntimeError(
                            "SuperTable catalog root changed during migration "
                            "preflight"
                        )
                    if (
                        planned_migrations
                        > _MAX_REDIS_ROOT_VERSION - root_version
                    ):
                        raise ValueError(
                            "Offline migration lacks Redis root version headroom"
                        )
        finally:
            self.catalog.release_namespace_lock(self.organization, self.super_name, token)
        return {"super_name": self.super_name, "organization": self.organization, "migrated_tables": migrated}

    def _migrate_legacy_leaf(
        self,
        *,
        simple: str,
        leaf: Dict[str, Any],
        namespace_token: str,
        simple_token: str,
        mirror_pin: Optional[str],
        preflight_only: bool = False,
    ) -> bool:
        from supertable.processing import (
            MAX_SHOW_STATS_DECODED_BYTES,
            MAX_SHOW_STATS_ROWS,
            extract_stats_rows,
            build_stats_file,
            load_bounded_stats_diagnostic,
            resource_stats_seal,
            stats_resource_seals,
        )

        path = leaf.get("path")
        if not isinstance(path, str) or not path:
            raise RuntimeError(f"Table {simple} has no snapshot path")
        version = leaf.get("version")
        if type(version) is not int or version < 0:
            raise RuntimeError(f"Table {simple} has an invalid snapshot version")
        from supertable.simple_table import (
            _contained_artifact_path,
            _object_seal_document,
            _read_sealed_json_object,
            _validate_physical_containment,
        )

        table_dir = os.path.join(
            self.organization, self.super_name, "tables", simple,
        )
        snapshot_dir = os.path.join(table_dir, "snapshots")
        path = _contained_artifact_path(
            path,
            label="live migration snapshot",
            required_prefix=snapshot_dir,
        )
        _validate_physical_containment(
            self.storage, path, snapshot_dir,
        )
        snapshot, _snapshot_metadata = _read_sealed_json_object(
            self.storage,
            path,
            max_bytes=_MAX_MIGRATION_SNAPSHOT_BYTES,
            label="Migration snapshot",
        )
        if not isinstance(snapshot, dict):
            raise ValueError(f"Table {simple} snapshot is not an object")
        from supertable.utils.snapshot import (
            complete_snapshot_payload,
            snapshot_cache_payload,
        )
        from supertable.tombstone_manifest_v2 import (
            MAX_JSON_EXACT_INTEGER,
            TOMBSTONE_FORMAT_V2,
            TOMBSTONE_FORMAT_V3,
        )

        def validate_successor_publication_size(
            document: Dict[str, Any],
        ) -> None:
            pending_values: list[Any] = [document]
            while pending_values:
                value = pending_values.pop()
                if isinstance(value, dict):
                    pending_values.extend(value.values())
                elif isinstance(value, list):
                    pending_values.extend(value)
                elif (
                    type(value) is int
                    and abs(value) > MAX_JSON_EXACT_INTEGER
                ):
                    raise ValueError(
                        f"Table {simple} migration successor exceeds the "
                        "Redis JSON safe integer range"
                    )
            try:
                raw_schema = document.get("schema", {})
                if isinstance(raw_schema, dict):
                    schema_document = dict(raw_schema)
                elif isinstance(raw_schema, list):
                    schema_document = {}
                    for schema_item in raw_schema:
                        if isinstance(schema_item, dict):
                            schema_document.update(schema_item)
                else:
                    schema_document = {}
                schema_json = json.dumps(
                    schema_document,
                    allow_nan=False,
                )
                redis_document = json.dumps(
                    snapshot_cache_payload(document),
                    allow_nan=False,
                )
                # Built-in object stores use either compact/default JSON or
                # LocalStorage's indented UTF-8 form. An indented ASCII form
                # is an upper bound for both without relying on a backend's
                # private encoder.
                stored_document = json.dumps(
                    document,
                    allow_nan=False,
                    indent=2,
                    ensure_ascii=True,
                ).encode("ascii")
            except (TypeError, ValueError, UnicodeEncodeError):
                raise ValueError(
                    f"Table {simple} migration successor is not JSON serializable"
                ) from None
            if (
                len(redis_document) > _MAX_MIGRATION_SNAPSHOT_BYTES
                or len(stored_document) > _MAX_MIGRATION_SNAPSHOT_BYTES
            ):
                raise ValueError(
                    f"Table {simple} migration successor snapshot exceeds its "
                    "publication size limit"
                )
            if len(schema_json) > _MAX_MIGRATION_SCHEMA_BYTES:
                raise ValueError(
                    f"Table {simple} migration successor schema exceeds its "
                    "publication size limit"
                )

        rowid_high_watermark = snapshot.get("rowid_high_watermark")
        if (
            type(rowid_high_watermark) is int
            and abs(rowid_high_watermark) > MAX_JSON_EXACT_INTEGER
        ):
            raise ValueError(
                f"Table {simple} snapshot exceeds the Redis JSON safe "
                "integer range"
            )

        def redis_lua_comparison_value(value: Any) -> Any:
            # The commit scripts decode and re-encode the payload with Redis
            # Lua cjson. Model its two known lossy lanes on both sides: an
            # empty object becomes an empty array, and very large integers
            # collapse to their double representation. Differences outside
            # the same unavoidable equivalence bucket remain visible.
            if isinstance(value, dict):
                if not value:
                    return []
                return {
                    key: redis_lua_comparison_value(item)
                    for key, item in value.items()
                }
            if isinstance(value, list):
                return [
                    redis_lua_comparison_value(item)
                    for item in value
                ]
            if type(value) is int and abs(value) > (1 << 53) - 1:
                return float(value)
            return value

        def cache_comparison_document(document: Dict[str, Any]) -> Any:
            return redis_lua_comparison_value(
                snapshot_cache_payload(document),
            )

        legacy_v2_4_snapshot = self._is_v2_4_snapshot(snapshot)
        preserves_v2_4_schema_semantics = (
            legacy_v2_4_snapshot
            or snapshot.get("_legacy_metadata_migration_version")
            == _LEGACY_METADATA_MIGRATION_VERSION
        )
        cached_payload = leaf.get("payload")
        if legacy_v2_4_snapshot:
            if cached_payload is None:
                cached_snapshot = None
            elif not isinstance(cached_payload, dict):
                raise RuntimeError(
                    f"Table {simple} v2.4 Redis snapshot cache is invalid"
                )
            else:
                nested_cached_payload = cached_payload.get("snapshot")
                cached_snapshot = (
                    nested_cached_payload
                    if (
                        not isinstance(cached_payload.get("resources"), list)
                        and isinstance(nested_cached_payload, dict)
                    )
                    else cached_payload
                )
        else:
            cached_snapshot = complete_snapshot_payload(
                cached_payload,
                expected_version=version,
                require_policy_marker=True,
            )
        if cached_snapshot is not None:

            if cache_comparison_document(
                cached_snapshot,
            ) != cache_comparison_document(snapshot):
                raise RuntimeError(
                    f"Table {simple} Redis snapshot cache disagrees with storage"
                )
        from supertable.row_identity import snapshot_proves_stable_rowids
        preserve_current_rowids = snapshot_proves_stable_rowids(snapshot)
        migration_required = self._legacy_metadata_migration_required(
            simple=simple,
            table_dir=table_dir,
            version=version,
            snapshot=snapshot,
        )
        if not migration_required:
            # A snapshot can be treated as current only after its mandatory
            # footer/statistics seals have been checked against immutable
            # objects. Redis cache state, marker presence, and field names
            # alone are not an authority boundary.
            current_resources, _footer_cache, canonical_schema = (
                self._validate_legacy_resources(
                    simple=simple,
                    snapshot=snapshot,
                    allow_v2_4_timestamp_only=(
                        preserves_v2_4_schema_semantics
                    ),
                )
            )
            from supertable.simple_table import (
                _contained_artifact_path,
                _restored_schema_field_names,
                _restored_schema_type_values,
                _validate_physical_containment,
            )
            from supertable.processing import (
                load_bounded_stats_diagnostic,
                resource_stats_seal,
                stats_resource_seals,
            )

            current_schema = snapshot.get("schema")
            current_schema_names = _restored_schema_field_names(current_schema)
            declared_schema = _restored_schema_type_values(
                current_schema,
                current_schema_names,
            )
            rebuild_current = declared_schema != canonical_schema
            stats_file = snapshot.get("stats_file")
            expected_stats_seals = {
                resource["file"]: seal
                for resource in current_resources
                for seal in [resource_stats_seal(resource)]
                if seal is not None and seal.stats_rows > 0
            }
            if stats_file is None and expected_stats_seals:
                rebuild_current = True
            if stats_file is not None:
                stats_dir = os.path.join(table_dir, "stats")
                try:
                    stats_path = _contained_artifact_path(
                        stats_file,
                        label="current migration statistics",
                        required_prefix=stats_dir,
                    )
                    _validate_physical_containment(
                        self.storage,
                        stats_path,
                        stats_dir,
                    )
                    stats_frame = load_bounded_stats_diagnostic(
                        stats_path,
                        expected_rows=snapshot["stats_rows"],
                        storage=self.storage,
                    )
                    if stats_resource_seals(stats_frame) != expected_stats_seals:
                        rebuild_current = True
                except (FileNotFoundError, RuntimeError, TypeError, ValueError):
                    rebuild_current = True
            if not rebuild_current:
                self._migrate_legacy_tombstone(
                    simple=simple,
                    snapshot=copy.deepcopy(snapshot),
                    version=version,
                    allowed_files={
                        resource["file"] for resource in current_resources
                    },
                    available_rows=sum(
                        resource["rows"] for resource in current_resources
                    ),
                    publish_successor=False,
                )
                return False
        successor = copy.deepcopy(snapshot)
        successor["simple_name"] = simple
        successor["location"] = table_dir
        resources, footer_cache, canonical_schema = (
            self._validate_legacy_resources(
                simple=simple,
                snapshot=successor,
                allow_v2_4_timestamp_only=(
                    preserves_v2_4_schema_semantics
                ),
            )
        )
        successor["resources"] = resources
        successor["schema"] = canonical_schema
        if legacy_v2_4_snapshot:
            successor["schemaString"] = json.dumps(
                {"type": "struct", "fields": canonical_schema},
                separators=(",", ":"),
            )
            successor["_row_filter"] = None
        data_paths = [resource["file"] for resource in resources]
        # A legacy snapshot cannot acquire stable-row-ID authority from metadata
        # alone.  Every current writer seal is tied to its immutable footer, but
        # migration cannot prove an old digest without scanning data pages.
        if preserve_current_rowids:
            original_resources = {
                resource["file"]: resource
                for resource in snapshot["resources"]
            }
            for resource in resources:
                original = original_resources[resource["file"]]
                resource["rowid_integrity"] = copy.deepcopy(
                    original["rowid_integrity"],
                )
            if not snapshot_proves_stable_rowids(successor):
                raise RuntimeError(
                    f"Table {simple} stable row-ID proof could not be preserved"
                )
        else:
            successor.pop("rowid_high_watermark", None)

        rowid_index = None
        rowid_index_directory = None
        legacy_rowid_facts: dict[str, Dict[str, Any]] = {}
        legacy_binary_value_bounds: dict[str, Dict[str, int]] = {}
        legacy_rowid_high_watermark = 0
        legacy_allocator_high_watermark = None
        if legacy_v2_4_snapshot and resources:
            (
                rowid_index_directory,
                rowid_index,
                legacy_rowid_facts,
                legacy_rowid_high_watermark,
                legacy_binary_value_bounds,
            ) = self._scan_v2_4_resources(
                simple=simple,
                resources=resources,
                footer_cache=footer_cache,
            )
        if legacy_v2_4_snapshot:
            legacy_allocator_high_watermark = (
                self._read_v2_4_rowid_sequence(simple=simple)
            )
            migrated_rowid_high_watermark = max(
                legacy_rowid_high_watermark,
                legacy_allocator_high_watermark,
            )
            if migrated_rowid_high_watermark > MAX_JSON_EXACT_INTEGER:
                raise ValueError(
                    f"Table {simple} v2.4 row IDs exceed the current Redis "
                    "snapshot limit"
                )
            successor["rowid_high_watermark"] = migrated_rowid_high_watermark
            for resource in resources:
                facts = legacy_rowid_facts.get(resource["file"])
                if facts is None:
                    raise RuntimeError(
                        f"Table {simple} lacks a verified v2.4 row-ID seal"
                    )
                resource["rowid_integrity"] = facts
                binary_bounds = legacy_binary_value_bounds.get(resource["file"])
                if binary_bounds:
                    resource["column_max_value_bytes"] = binary_bounds
            if not snapshot_proves_stable_rowids(successor):
                raise RuntimeError(
                    f"Table {simple} v2.4 row-ID proof is not write-ready"
                )
        try:
            self._migrate_legacy_tombstone(
                simple=simple,
                snapshot=successor,
                version=version,
                allowed_files=set(data_paths),
                available_rows=sum(resource["rows"] for resource in resources),
                publish_successor=not preflight_only,
                legacy_rowid_index=rowid_index,
            )
        finally:
            if rowid_index is not None:
                rowid_index.close()
            if rowid_index_directory is not None:
                rowid_index_directory.cleanup()

        if legacy_v2_4_snapshot:
            projected_stats_rows = 0
            projected_stats_bytes = 0
            try:
                for resource in resources:
                    resource_path = resource["file"]
                    encoded_resource_path = resource_path.encode("utf-8")
                    metadata = footer_cache[resource_path]
                    projected_stats_bytes += max(
                        0, int(getattr(metadata, "serialized_size", 0)),
                    ) * 2
                    for row_group_id in range(metadata.num_row_groups):
                        row_group = metadata.row_group(row_group_id)
                        for column_id in range(row_group.num_columns):
                            column_name = str(
                                row_group.column(column_id).path_in_schema,
                            )
                            if column_name in {
                                "__rowid__", "__timestamp__",
                            }:
                                continue
                            encoded_column_name = column_name.encode("utf-8")
                            projected_stats_rows += 1
                            # During extraction a Python dict/string view and
                            # the resulting Polars buffers coexist. Charge a
                            # conservative fixed object cost plus three copies
                            # of the two repeated identity strings.
                            projected_stats_bytes += 1024 + 3 * (
                                len(encoded_resource_path)
                                + len(encoded_column_name)
                            )
                            if (
                                projected_stats_rows
                                > _MAX_MIGRATION_STATS_ROWS
                                or projected_stats_bytes
                                > _MAX_MIGRATION_STATS_DECODED_BYTES
                            ):
                                raise ValueError
            except ValueError:
                raise ValueError(
                    f"Table {simple} statistics materialization exceeds the "
                    "offline migration memory bound"
                ) from None
            except Exception:
                raise RuntimeError(
                    f"Table {simple} statistics materialization could not be "
                    "bounded"
                ) from None

        if legacy_v2_4_snapshot:
            stats_rows = extract_stats_rows(
                data_paths,
                footer_md_cache=footer_cache,
                storage=self.storage,
            )
        else:
            stats_rows = extract_stats_rows(
                data_paths,
                footer_md_cache=footer_cache,
                storage=self.storage,
                max_rows=MAX_SHOW_STATS_ROWS,
                max_decoded_bytes=MAX_SHOW_STATS_DECODED_BYTES,
            )
        if legacy_v2_4_snapshot:
            try:
                live_stats_bytes = int(stats_rows.estimated_size())
            except Exception:
                raise RuntimeError(
                    f"Table {simple} statistics decoded size is unavailable"
                ) from None
            if (
                live_stats_bytes < 0
                or live_stats_bytes * 3
                > _MAX_MIGRATION_STATS_DECODED_BYTES
            ):
                raise ValueError(
                    f"Table {simple} statistics materialization exceeds the "
                    "offline migration memory bound"
                )
        expected_stats_seals = {
            resource["file"]: seal
            for resource in resources
            for seal in [resource_stats_seal(resource)]
            if seal is not None and seal.stats_rows > 0
        }
        source_stats_seals = stats_resource_seals(stats_rows)
        if (
            source_stats_seals is None
            and not data_paths
            and getattr(stats_rows, "height", None) == 0
        ):
            # Some compatibility implementations represent an empty table as
            # a schema-less empty frame. There is no resource seal to lose.
            source_stats_seals = {}
        if source_stats_seals != expected_stats_seals:
            raise RuntimeError(
                f"Table {simple} source statistics failed seal validation"
            )
        if preflight_only:
            projected_successor = copy.deepcopy(successor)
            if legacy_v2_4_snapshot:
                if projected_successor.get("tombstone") is None:
                    projected_successor["tombstone"] = None
                    projected_successor["tombstone_rows"] = 0
                    projected_successor["tombstone_digest"] = None
                else:
                    projected_successor["tombstone"] = os.path.join(
                        table_dir,
                        "tombstone",
                        "year=9999/month=99/day=99/hour=99",
                        generate_filename(
                            alias="deleted-v3",
                            extension="parquet",
                        ),
                    )
                    projected_successor["tombstone_digest"] = "0" * 64
                projected_successor["tombstone_format"] = TOMBSTONE_FORMAT_V3
            elif (
                projected_successor.get("tombstone_format")
                == TOMBSTONE_FORMAT_V2
                and projected_successor.get("tombstone") is not None
            ):
                # A validated v2 root is republished for the successor
                # generation.  Project its new partitioned pointer, not the
                # possibly much shorter source pointer, so pass one applies
                # the same publication bound as the real commit pass.
                projected_successor["tombstone"] = os.path.join(
                    table_dir,
                    "tombstone",
                    "year=9999/month=99/day=99/hour=99",
                    generate_filename(alias="manifest", extension="json"),
                )
                projected_successor["tombstone_digest"] = "0" * 64
            stats_row_count = int(getattr(stats_rows, "height", 0))
            projected_successor["stats_file"] = (
                os.path.join(
                    table_dir,
                    "stats",
                    "year=9999/month=99/day=99/hour=99",
                    generate_filename(alias="stats", extension="parquet"),
                )
                if stats_row_count > 0
                else None
            )
            projected_successor["stats_rows"] = stats_row_count
            projected_successor["snapshot_version"] = version + 1
            projected_successor["previous_snapshot"] = path
            projected_successor["last_updated_ms"] = int(time.time() * 1000)
            # Provider nanosecond timestamps are not safe in Redis Lua cjson.
            # Object seals are optional read optimizations; footer and stats
            # seals remain authoritative, so never publish this lossy field.
            for projected_resource in projected_successor["resources"]:
                projected_resource.pop("object_seal", None)
            if not snapshot_proves_stable_rowids(projected_successor):
                raise ValueError(
                    f"Table {simple} migration cannot establish a stable "
                    "row-ID proof"
                )
            projected_successor["_legacy_metadata_migration_version"] = (
                _LEGACY_METADATA_MIGRATION_VERSION
            )
            validate_successor_publication_size(projected_successor)
            return True
        generated_stats_path, combined = build_stats_file(
            stats_dir=f"{self.organization}/{self.super_name}/tables/{simple}/stats",
            prev_stats_path=None,
            new_rows=stats_rows,
            removed_files=set(),
            compression_level=3,
            storage=self.storage,
        )
        stats_row_count = int(combined.height) if combined is not None else 0
        if generated_stats_path is not None:
            if legacy_v2_4_snapshot:
                import polars as pl
                import pyarrow as pa

                from supertable.storage.storage_interface import ObjectMetadata

                if combined is None:
                    raise RuntimeError(
                        f"Table {simple} generated statistics are unavailable"
                    )
                iter_stats_batches = getattr(
                    self.storage, "iter_parquet_batches", None,
                )
                if not callable(iter_stats_batches):
                    raise RuntimeError(
                        "Offline v2.4 migration requires bounded statistics "
                        "readback"
                    )
                observed_stats = self.storage.stat_object(generated_stats_path)
                if (
                    not isinstance(observed_stats, ObjectMetadata)
                    or observed_stats.size <= 0
                    or not observed_stats.identity_token()
                ):
                    raise RuntimeError(
                        f"Table {simple} generated statistics have no immutable "
                        "identity"
                    )
                decoded_rows = 0
                try:
                    for raw_batch in iter_stats_batches(
                        generated_stats_path,
                        max_decoded_bytes=8 * 1024 * 1024,
                        columns=None,
                    ):
                        if isinstance(raw_batch, pa.RecordBatch):
                            table = pa.Table.from_batches([raw_batch])
                        elif isinstance(raw_batch, pa.Table):
                            table = raw_batch
                        else:
                            raise RuntimeError
                        table.validate(full=True)
                        if (
                            table.num_rows <= 0
                            or table.nbytes > 8 * 1024 * 1024
                            or decoded_rows + table.num_rows > stats_row_count
                        ):
                            raise RuntimeError
                        observed_frame = pl.from_arrow(table)
                        expected_frame = combined.slice(
                            decoded_rows, table.num_rows,
                        )
                        if (
                            observed_frame.schema != combined.schema
                            or not observed_frame.equals(expected_frame)
                        ):
                            raise RuntimeError
                        decoded_rows += int(table.num_rows)
                except Exception:
                    raise RuntimeError(
                        f"Table {simple} generated statistics failed bounded "
                        "readback"
                    ) from None
                resealed_stats = self.storage.stat_object(generated_stats_path)
                if (
                    decoded_rows != stats_row_count
                    or not isinstance(resealed_stats, ObjectMetadata)
                    or resealed_stats != observed_stats
                ):
                    raise RuntimeError(
                        f"Table {simple} generated statistics changed during "
                        "bounded readback"
                    )
                validated_stats = combined
            else:
                validated_stats = load_bounded_stats_diagnostic(
                    generated_stats_path,
                    expected_rows=stats_row_count,
                    storage=self.storage,
                    max_rows=MAX_SHOW_STATS_ROWS,
                    max_decoded_bytes=MAX_SHOW_STATS_DECODED_BYTES,
                )
            if stats_resource_seals(validated_stats) != expected_stats_seals:
                raise RuntimeError(
                    f"Table {simple} generated statistics failed seal validation"
                )
        successor["stats_file"] = generated_stats_path
        successor["stats_rows"] = stats_row_count
        successor["snapshot_version"] = version + 1
        successor["previous_snapshot"] = path
        now_ms = int(time.time() * 1000)
        successor["last_updated_ms"] = now_ms
        new_path = f"{self.organization}/{self.super_name}/tables/{simple}/snapshots/{generate_filename(alias=simple)}"
        commit_id = uuid.uuid4().hex
        if legacy_v2_4_snapshot:
            from supertable.storage.storage_interface import ObjectMetadata

            for resource in resources:
                observed = self.storage.stat_object(resource["file"])
                if (
                    not isinstance(observed, ObjectMetadata)
                    or _object_seal_document(observed)
                    != resource.get("object_seal")
                ):
                    raise RuntimeError(
                        f"Table {simple} data resource changed after its full scan"
                    )
        # The object seal was useful while validating the source but is an
        # optional read optimization.  In particular, provider nanosecond
        # timestamps exceed Redis Lua cjson's exact integer range and must not
        # be carried into the cache in rounded form.
        for resource in resources:
            resource.pop("object_seal", None)
        if not snapshot_proves_stable_rowids(successor):
            raise ValueError(
                f"Table {simple} migration cannot establish a stable row-ID proof"
            )
        successor["_legacy_metadata_migration_version"] = (
            _LEGACY_METADATA_MIGRATION_VERSION
        )
        validate_successor_publication_size(successor)
        if (
            legacy_allocator_high_watermark is not None
            and self._read_v2_4_rowid_sequence(simple=simple)
            != legacy_allocator_high_watermark
        ):
            raise RuntimeError(
                f"Table {simple} v2.4 row-ID allocator changed during migration"
            )
        if (
            successor.get("_legacy_metadata_migration_version")
            != _LEGACY_METADATA_MIGRATION_VERSION
            or not snapshot_proves_stable_rowids(successor)
        ):
            raise RuntimeError(
                f"Table {simple} migration proof changed before publication"
            )
        from supertable.storage.storage_interface import ObjectMetadata

        current_snapshot_metadata = self.storage.stat_object(path)
        if (
            not isinstance(current_snapshot_metadata, ObjectMetadata)
            or current_snapshot_metadata != _snapshot_metadata
        ):
            raise RuntimeError(
                f"Table {simple} source snapshot changed before publication"
            )
        self.storage.write_json(new_path, successor)
        try:
            written_successor, _written_metadata = _read_sealed_json_object(
                self.storage,
                new_path,
                max_bytes=_MAX_MIGRATION_SNAPSHOT_BYTES,
                label="Generated migration snapshot",
            )
        except Exception:
            raise RuntimeError(
                f"Table {simple} generated snapshot failed readback"
            ) from None
        if written_successor != successor:
            raise RuntimeError(
                f"Table {simple} generated snapshot failed readback"
            )
        try:
            self.catalog.commit_snapshot(
                self.organization,
                self.super_name,
                simple,
                successor,
                new_path,
                expected_version=version,
                expected_path=path,
                lock_token=simple_token,
                commit_id=commit_id,
                now_ms=now_ms,
                namespace_token=namespace_token,
                expected_mirrors=(),
                expected_mirror_pin=mirror_pin,
            )
        except Exception as commit_error:
            import redis

            if not isinstance(commit_error, redis.RedisError):
                raise
            reconciled = self.catalog.get_leaf(
                self.organization, self.super_name, simple,
            )
            if not (
                isinstance(reconciled, dict)
                and reconciled.get("version") == version + 1
                and reconciled.get("path") == new_path
                and reconciled.get("commit_id") == commit_id
                and isinstance(reconciled.get("payload"), dict)
                and cache_comparison_document(reconciled["payload"])
                == cache_comparison_document(successor)
            ):
                raise
        return True

    @staticmethod
    def _is_v2_4_snapshot(snapshot: Dict[str, Any]) -> bool:
        """Recognize only the authoritative snapshot shape written by v2.4."""
        if not isinstance(snapshot, dict) or any(
            field in snapshot
            for field in (
                "tombstone_digest",
                "tombstone_format",
                "rowid_high_watermark",
                "_row_filter",
                "_legacy_metadata_migration_version",
            )
        ):
            return False
        if not (
            "tombstone" in snapshot
            and "tombstone_rows" in snapshot
            and isinstance(snapshot.get("resources"), list)
        ):
            return False
        schema = snapshot.get("schema")
        if not snapshot["resources"]:
            if schema == []:
                return True
        if not isinstance(schema, dict):
            return False
        reserved_system_names = {
            name
            for name in schema
            if isinstance(name, str)
            and name.casefold() in {"__rowid__", "__timestamp__"}
        }
        # v2.4 reserved row IDs only for non-empty writes, but injected the
        # timestamp column before updating the table schema.  Consequently an
        # empty final write can authoritatively leave timestamp without rowid.
        return reserved_system_names in (
            {"__timestamp__"},
            {"__rowid__", "__timestamp__"},
        )

    def _scan_v2_4_resources(
        self,
        *,
        simple: str,
        resources: list[Dict[str, Any]],
        footer_cache: dict[str, Any],
    ) -> tuple[
        Any,
        Any,
        dict[str, Dict[str, Any]],
        int,
        dict[str, Dict[str, int]],
    ]:
        """Fully decode v2.4 data and build an exact disk-backed row-ID index."""
        import hashlib
        import select
        import shutil
        import sqlite3
        import stat
        import subprocess
        import sys
        import tempfile

        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.parquet as pq

        from supertable.processing import _stats_rows_for_metadata
        from supertable.row_identity import MAX_TABLE_ROWID
        from supertable.simple_table import _object_seal_document
        from supertable.storage.storage_interface import ObjectMetadata

        iter_batches = getattr(self.storage, "iter_parquet_batches", None)
        if not callable(iter_batches):
            raise RuntimeError(
                "Offline v2.4 migration requires bounded Parquet batch reads"
            )

        # The bounded storage reader spills one complete compressed resource,
        # while this migration maintains two integer-only SQLite indexes (all
        # physical row IDs and, transiently, deletion-vector membership).
        # Reserve a deliberately conservative 96 bytes per physical row plus
        # fixed SQLite/filesystem headroom before beginning the first scan.
        required_temporary_bytes = (
            max((resource["file_size"] for resource in resources), default=0)
            + sum(resource["rows"] for resource in resources) * 96
            + 256 * 1024 * 1024
        )
        try:
            free_temporary_bytes = shutil.disk_usage(
                tempfile.gettempdir(),
            ).free
        except OSError:
            raise RuntimeError(
                f"Table {simple} temporary disk capacity could not be verified"
            ) from None
        if free_temporary_bytes < required_temporary_bytes:
            raise RuntimeError(
                f"Table {simple} has insufficient temporary disk capacity "
                "for the offline migration"
            )

        directory = tempfile.TemporaryDirectory(
            prefix="supertable-v2-4-migration-",
        )
        connection = sqlite3.connect(
            os.path.join(directory.name, "rowids.sqlite3"),
        )
        active_batches = None
        try:
            connection.execute("PRAGMA journal_mode=OFF")
            connection.execute("PRAGMA synchronous=OFF")
            connection.execute("PRAGMA temp_store=FILE")
            connection.execute(
                "CREATE TABLE resources ("
                "id INTEGER PRIMARY KEY, file TEXT NOT NULL UNIQUE"
                ")"
            )
            connection.execute(
                "CREATE TABLE rowids ("
                "value INTEGER PRIMARY KEY, resource_id INTEGER NOT NULL"
                ") WITHOUT ROWID"
            )
            facts: dict[str, Dict[str, Any]] = {}
            variable_value_bounds: dict[str, Dict[str, int]] = {}
            high_watermark = 0
            expected_timestamp_type = pa.timestamp("us", tz="UTC")
            digest_domain = b"supertable-rowid-integrity-v1\0"

            for resource_id, resource in enumerate(resources, start=1):
                path = resource["file"]
                connection.execute(
                    "INSERT INTO resources(id, file) VALUES (?, ?)",
                    (resource_id, path),
                )
                declared_rows = resource["rows"]
                parquet_metadata = footer_cache.get(path)
                if parquet_metadata is None:
                    raise RuntimeError(
                        f"Table {simple} has no pinned footer for a data resource"
                    )
                created_by = str(parquet_metadata.created_by or "")
                if created_by.startswith("parquet-cpp-arrow version "):
                    source_parquet_writer = "arrow"
                elif created_by == "Polars" or created_by.startswith("Polars "):
                    source_parquet_writer = "polars"
                else:
                    raise RuntimeError(
                        f"Table {simple} v2.4 resource has an unsupported "
                        "Parquet writer"
                    )
                arrow_schema = parquet_metadata.schema.to_arrow_schema()

                def contains_fixed_size_list(
                    dtype: Any,
                    depth: int = 0,
                ) -> bool:
                    if depth > 64:
                        raise ValueError(
                            f"Table {simple} physical schema is nested too deeply"
                        )
                    if pa.types.is_fixed_size_list(dtype):
                        return True
                    if pa.types.is_struct(dtype):
                        return any(
                            contains_fixed_size_list(field.type, depth + 1)
                            for field in dtype
                        )
                    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
                        return contains_fixed_size_list(
                            dtype.value_type, depth + 1,
                        )
                    if pa.types.is_map(dtype):
                        return (
                            contains_fixed_size_list(dtype.key_type, depth + 1)
                            or contains_fixed_size_list(
                                dtype.item_type, depth + 1,
                            )
                        )
                    return False

                requires_polars_scan = any(
                    contains_fixed_size_list(field.type)
                    for field in arrow_schema
                )

                def fixed_decoded_row_bytes(
                    dtype: Any,
                    depth: int = 0,
                ) -> Optional[int]:
                    if depth > 64:
                        raise ValueError(
                            f"Table {simple} physical schema is nested too deeply"
                        )
                    if pa.types.is_null(dtype):
                        return 1
                    if pa.types.is_boolean(dtype):
                        return 2
                    if (
                        pa.types.is_integer(dtype)
                        or pa.types.is_floating(dtype)
                        or pa.types.is_decimal(dtype)
                        or pa.types.is_date(dtype)
                        or pa.types.is_time(dtype)
                        or pa.types.is_timestamp(dtype)
                        or pa.types.is_duration(dtype)
                    ):
                        return max(1, int(dtype.bit_width) // 8) + 1
                    if pa.types.is_fixed_size_binary(dtype):
                        return int(dtype.byte_width) + 1
                    if pa.types.is_fixed_size_list(dtype):
                        child_bytes = fixed_decoded_row_bytes(
                            dtype.value_type, depth + 1,
                        )
                        if child_bytes is None:
                            return None
                        return 1 + int(dtype.list_size) * child_bytes
                    if pa.types.is_struct(dtype):
                        child_widths = [
                            fixed_decoded_row_bytes(field.type, depth + 1)
                            for field in dtype
                        ]
                        if any(width is None for width in child_widths):
                            return None
                        return 1 + sum(
                            width
                            for width in child_widths
                            if width is not None
                        )
                    return None

                fixed_schema_widths = [
                    fixed_decoded_row_bytes(field.type)
                    for field in arrow_schema
                ]
                polars_scan_chunk_rows = 1
                if (
                    requires_polars_scan
                    and all(width is not None for width in fixed_schema_widths)
                ):
                    fixed_row_bytes = max(
                        1,
                        sum(
                            width
                            for width in fixed_schema_widths
                            if width is not None
                        ),
                    )
                    # Keep the native frame, its Arrow projection, and proof
                    # encoder working set independently below 8 MiB.
                    polars_scan_chunk_rows = max(
                        1,
                        min(4096, (2 * 1024 * 1024) // fixed_row_bytes),
                    )

                requires_polars_only_proof = (
                    source_parquet_writer == "polars"
                    and requires_polars_scan
                )
                try:
                    rowid_field = arrow_schema.field("__rowid__")
                    timestamp_field = arrow_schema.field("__timestamp__")
                except KeyError:
                    raise ValueError(
                        f"Table {simple} v2.4 resource lacks system columns"
                    ) from None
                if (
                    rowid_field.type != pa.int64()
                    or timestamp_field.type != expected_timestamp_type
                ):
                    raise ValueError(
                        f"Table {simple} v2.4 resource has invalid system column types"
                    )
                resource_variable_bounds = {
                    str(field.name): 0
                    for field in arrow_schema
                    if (
                        pa.types.is_binary(field.type)
                        or pa.types.is_large_binary(field.type)
                        or pa.types.is_fixed_size_binary(field.type)
                        or pa.types.is_string(field.type)
                        or pa.types.is_large_string(field.type)
                    )
                }
                footer_proofs_by_group: list[
                    dict[
                        str,
                        tuple[
                            dict[str, Any],
                            Optional[str],
                            Any,
                            Any,
                        ],
                    ]
                ] = [
                    {} for _ in range(parquet_metadata.num_row_groups)
                ]
                footer_stats_rows = _stats_rows_for_metadata(
                    path,
                    parquet_metadata,
                    footer_sha256=resource["footer_sha256"],
                )
                lane_fields = (
                    ("bigint", "min_bigint", "max_bigint"),
                    ("double", "min_double", "max_double"),
                    ("timestamp", "min_timestamp", "max_timestamp"),
                    ("string", "min_string", "max_string"),
                )

                def usable_stats_range(
                    stats_row: dict[str, Any],
                ) -> tuple[Optional[str], Any, Any]:
                    lane: Optional[str] = None
                    stored_minimum = None
                    stored_maximum = None
                    if stats_row.get("stats_available") is True:
                        populated = [
                            (
                                candidate_lane,
                                stats_row.get(minimum),
                                stats_row.get(maximum),
                            )
                            for candidate_lane, minimum, maximum in lane_fields
                            if (
                                stats_row.get(minimum) is not None
                                or stats_row.get(maximum) is not None
                            )
                        ]
                        if len(populated) == 1:
                            candidate_lane, candidate_minimum, candidate_maximum = (
                                populated[0]
                            )
                            try:
                                usable_range = (
                                    candidate_minimum is not None
                                    and candidate_maximum is not None
                                    and candidate_minimum == candidate_minimum
                                    and candidate_maximum == candidate_maximum
                                    and candidate_minimum <= candidate_maximum
                                )
                            except Exception:
                                usable_range = False
                            if usable_range:
                                lane = candidate_lane
                                stored_minimum = candidate_minimum
                                stored_maximum = candidate_maximum
                    return lane, stored_minimum, stored_maximum

                for stats_row in footer_stats_rows:
                    column_name = stats_row.get("column_name")
                    group_id = stats_row.get("row_group_id")
                    if (
                        not isinstance(column_name, str)
                        or not column_name
                        or type(group_id) is not int
                        or not 0 <= group_id < parquet_metadata.num_row_groups
                    ):
                        raise RuntimeError(
                            f"Table {simple} footer statistics cannot be "
                            "verified from decoded data"
                        )
                    lane, stored_minimum, stored_maximum = usable_stats_range(
                        stats_row,
                    )
                    group_proofs = footer_proofs_by_group[group_id]
                    if column_name in group_proofs:
                        raise RuntimeError(
                            f"Table {simple} footer statistics repeat a column"
                        )
                    group_proofs[column_name] = (
                        stats_row,
                        lane,
                        stored_minimum,
                        stored_maximum,
                    )

                row_group_sizes = [
                    int(parquet_metadata.row_group(index).num_rows)
                    for index in range(parquet_metadata.num_row_groups)
                ]
                if any(size < 0 for size in row_group_sizes) or sum(
                    row_group_sizes
                ) != declared_rows:
                    raise RuntimeError(
                        f"Table {simple} v2.4 row-group metadata is invalid"
                    )
                current_group = 0
                current_group_rows = 0
                group_accumulators: dict[str, dict[str, Any]] = {}

                def finish_row_group(group_id: int) -> None:
                    for column_name, footer_proof in (
                        footer_proofs_by_group[group_id].items()
                    ):
                        (
                            stats_row,
                            lane,
                            stored_minimum,
                            stored_maximum,
                        ) = footer_proof
                        accumulator = group_accumulators.get(column_name, {
                            "null_count": 0,
                            "null_count_known": True,
                            "nonnull_values": 0,
                            "nonnull_values_known": True,
                            "minimum": None,
                            "maximum": None,
                        })
                        expected_nulls = stats_row.get("null_count")
                        if (
                            type(expected_nulls) is int
                            and (
                                not accumulator["null_count_known"]
                                or accumulator["null_count"] != expected_nulls
                            )
                        ):
                            raise RuntimeError(
                                f"Table {simple} footer statistics disagree "
                                "with decoded data"
                            )
                        if lane is None:
                            continue
                        actual_minimum = accumulator["minimum"]
                        actual_maximum = accumulator["maximum"]
                        if actual_minimum is None or actual_maximum is None:
                            # Polars can publish dictionary-category bounds for
                            # an all-NULL Enum column.  Exact Arrow re-encoding
                            # proves whether the physical leaf truly has no
                            # non-NULL values; only then are absent page extrema
                            # compatible with those dictionary-only bounds.
                            if (
                                accumulator["nonnull_values_known"]
                                and accumulator["nonnull_values"] == 0
                            ) or lane == "double":
                                continue
                            raise RuntimeError(
                                f"Table {simple} footer statistics disagree "
                                "with decoded data"
                            )
                        try:
                            contains_pages = (
                                stored_minimum <= actual_minimum
                                and actual_maximum <= stored_maximum
                            )
                        except Exception:
                            contains_pages = False
                        if not contains_pages:
                            raise RuntimeError(
                                f"Table {simple} footer statistics disagree "
                                "with decoded data"
                            )

                def consume_group_slice(
                    group_id: int,
                    table_slice: Any,
                    polars_slice: Any = None,
                ) -> None:
                    def proof_rows(writer: str) -> list[dict[str, Any]]:
                        if writer == "arrow":
                            sink = pa.BufferOutputStream()
                            pq.write_table(
                                table_slice,
                                sink,
                                compression=None,
                                use_dictionary=True,
                                write_statistics=True,
                                row_group_size=max(
                                    1, int(table_slice.num_rows),
                                ),
                            )
                            proof_metadata = pq.read_metadata(
                                pa.BufferReader(sink.getvalue()),
                            )
                        else:
                            import io

                            import polars as pl

                            sink = io.BytesIO()
                            proof_frame = (
                                polars_slice
                                if polars_slice is not None
                                else pl.from_arrow(table_slice)
                            )
                            if (
                                not isinstance(proof_frame, pl.DataFrame)
                                or proof_frame.height != table_slice.num_rows
                            ):
                                raise RuntimeError
                            proof_frame.write_parquet(
                                sink,
                                compression="uncompressed",
                                statistics=True,
                                row_group_size=max(
                                    1, int(table_slice.num_rows),
                                ),
                            )
                            sink.seek(0)
                            proof_metadata = pq.read_metadata(sink)
                        if proof_metadata.num_row_groups != 1:
                            raise RuntimeError
                        rows = _stats_rows_for_metadata(
                            path,
                            proof_metadata,
                            footer_sha256=resource["footer_sha256"],
                        )
                        proof_nonnull: dict[str, int] = {}
                        proof_group = proof_metadata.row_group(0)
                        for column_index in range(proof_group.num_columns):
                            proof_column = proof_group.column(column_index)
                            proof_name = proof_column.path_in_schema
                            if proof_name in {"__rowid__", "__timestamp__"}:
                                continue
                            try:
                                proof_statistics = proof_column.statistics
                                nonnull_values = int(
                                    proof_statistics.num_values,
                                )
                            except Exception:
                                nonnull_values = -1
                            if proof_name in proof_nonnull:
                                nonnull_values = -1
                            proof_nonnull[proof_name] = nonnull_values
                        for row in rows:
                            proof_column_name = row.get("column_name")
                            row["__proof_nonnull_values"] = (
                                proof_nonnull.get(proof_column_name, -1)
                                if isinstance(proof_column_name, str)
                                else -1
                            )
                        return rows

                    try:
                        source_writer_rows = proof_rows(source_parquet_writer)
                        exact_range_rows = (
                            source_writer_rows
                            if (
                                source_parquet_writer == "arrow"
                                or requires_polars_only_proof
                            )
                            else proof_rows("arrow")
                        )
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except BaseException:
                        raise RuntimeError(
                            f"Table {simple} footer statistics could not "
                            "be verified from decoded data"
                        ) from None
                    def rows_by_name(
                        rows: list[dict[str, Any]],
                    ) -> dict[str, dict[str, Any]]:
                        result: dict[str, dict[str, Any]] = {}
                        for row in rows:
                            observed_name = row.get("column_name")
                            if (
                                not isinstance(observed_name, str)
                                or observed_name in result
                                or row.get("row_group_id") != 0
                            ):
                                raise RuntimeError(
                                    f"Table {simple} footer statistics could "
                                    "not be verified from decoded data"
                                )
                            result[observed_name] = row
                        return result

                    observed_by_name = rows_by_name(source_writer_rows)
                    exact_ranges_by_name = rows_by_name(exact_range_rows)
                    expected_proofs = footer_proofs_by_group[group_id]
                    if (
                        set(observed_by_name) != set(expected_proofs)
                        or set(exact_ranges_by_name) != set(expected_proofs)
                    ):
                        raise RuntimeError(
                            f"Table {simple} footer statistics could not "
                            "be verified from decoded data"
                        )
                    for column_name, proof in expected_proofs.items():
                        lane = proof[1]
                        observed_row = observed_by_name[column_name]
                        accumulator = group_accumulators.setdefault(
                            column_name,
                            {
                                "null_count": 0,
                                "null_count_known": True,
                                "nonnull_values": 0,
                                "nonnull_values_known": True,
                                "minimum": None,
                                "maximum": None,
                            },
                        )
                        observed_nulls = observed_row.get("null_count")
                        if type(observed_nulls) is int:
                            accumulator["null_count"] += observed_nulls
                        else:
                            accumulator["null_count_known"] = False
                        if lane is None:
                            continue
                        exact_range_row = exact_ranges_by_name[column_name]
                        observed_nonnull = exact_range_row.get(
                            "__proof_nonnull_values",
                        )
                        if type(observed_nonnull) is int and observed_nonnull >= 0:
                            accumulator["nonnull_values"] += observed_nonnull
                        else:
                            accumulator["nonnull_values_known"] = False
                        observed_lane, observed_minimum, observed_maximum = (
                            usable_stats_range(exact_range_row)
                        )
                        if observed_lane is None:
                            continue
                        if observed_lane != lane:
                            raise RuntimeError(
                                f"Table {simple} footer statistics disagree "
                                "with decoded data"
                            )
                        if (
                            accumulator["minimum"] is None
                            or observed_minimum < accumulator["minimum"]
                        ):
                            accumulator["minimum"] = observed_minimum
                        if (
                            accumulator["maximum"] is None
                            or observed_maximum > accumulator["maximum"]
                        ):
                            accumulator["maximum"] = observed_maximum

                while (
                    current_group < len(row_group_sizes)
                    and row_group_sizes[current_group] == 0
                ):
                    finish_row_group(current_group)
                    current_group += 1

                observed_before = self.storage.stat_object(path)
                if not isinstance(observed_before, ObjectMetadata):
                    raise RuntimeError(
                        f"Table {simple} data resource has no immutable identity"
                    )
                if (
                    _object_seal_document(observed_before)
                    != resource.get("object_seal")
                ):
                    raise RuntimeError(
                        f"Table {simple} data resource changed before its full scan"
                    )
                scanned_rows = 0
                minimum = None
                maximum = None
                digest = hashlib.sha256(digest_domain)

                def iter_polars_fixed_size_batches() -> Any:
                    def decode_page_header(
                        encoded: bytes,
                    ) -> tuple[int, int, int]:
                        position = 0
                        visited_values = 0

                        def read_byte() -> int:
                            nonlocal position
                            if position >= len(encoded):
                                raise ValueError
                            value = encoded[position]
                            position += 1
                            return value

                        def read_varint() -> int:
                            value = 0
                            for shift in range(0, 70, 7):
                                current = read_byte()
                                value |= (current & 0x7F) << shift
                                if not current & 0x80:
                                    return value
                            raise ValueError

                        def read_zigzag() -> int:
                            value = read_varint()
                            return (value >> 1) ^ -(value & 1)

                        def skip_bytes(length: int) -> None:
                            nonlocal position
                            if length < 0 or length > len(encoded) - position:
                                raise ValueError
                            position += length

                        def charge_value() -> None:
                            nonlocal visited_values
                            visited_values += 1
                            if visited_values > 100_000:
                                raise ValueError

                        def skip_value(
                            compact_type: int,
                            *,
                            field_value: bool,
                            depth: int,
                        ) -> None:
                            if depth > 64:
                                raise ValueError
                            charge_value()
                            if compact_type in {1, 2}:
                                if not field_value and read_byte() not in {1, 2}:
                                    raise ValueError
                            elif compact_type == 3:
                                read_byte()
                            elif compact_type in {4, 5, 6}:
                                read_varint()
                            elif compact_type == 7:
                                skip_bytes(8)
                            elif compact_type == 8:
                                skip_bytes(read_varint())
                            elif compact_type in {9, 10}:
                                header = read_byte()
                                length = header >> 4
                                element_type = header & 0x0F
                                if length == 15:
                                    length = read_varint()
                                if length > 100_000:
                                    raise ValueError
                                for _ in range(length):
                                    skip_value(
                                        element_type,
                                        field_value=False,
                                        depth=depth + 1,
                                    )
                            elif compact_type == 11:
                                length = read_varint()
                                if length > 100_000:
                                    raise ValueError
                                if length:
                                    types = read_byte()
                                    key_type = types >> 4
                                    value_type = types & 0x0F
                                    for _ in range(length):
                                        skip_value(
                                            key_type,
                                            field_value=False,
                                            depth=depth + 1,
                                        )
                                        skip_value(
                                            value_type,
                                            field_value=False,
                                            depth=depth + 1,
                                        )
                            elif compact_type == 12:
                                skip_struct(depth + 1)
                            else:
                                raise ValueError

                        def next_field(last_field: int) -> tuple[int, int]:
                            header = read_byte()
                            compact_type = header & 0x0F
                            if compact_type == 0:
                                return 0, last_field
                            delta = header >> 4
                            field_id = (
                                last_field + delta
                                if delta
                                else read_zigzag()
                            )
                            if field_id <= 0:
                                raise ValueError
                            return compact_type, field_id

                        def skip_struct(depth: int) -> None:
                            if depth > 64:
                                raise ValueError
                            last_field = 0
                            while True:
                                compact_type, field_id = next_field(last_field)
                                if compact_type == 0:
                                    return
                                last_field = field_id
                                skip_value(
                                    compact_type,
                                    field_value=True,
                                    depth=depth,
                                )

                        last_field = 0
                        uncompressed_size = None
                        compressed_size = None
                        while True:
                            compact_type, field_id = next_field(last_field)
                            if compact_type == 0:
                                break
                            last_field = field_id
                            if field_id in {2, 3} and compact_type == 5:
                                value = read_zigzag()
                                if field_id == 2:
                                    if uncompressed_size is not None:
                                        raise ValueError
                                    uncompressed_size = value
                                else:
                                    if compressed_size is not None:
                                        raise ValueError
                                    compressed_size = value
                            else:
                                skip_value(
                                    compact_type,
                                    field_value=True,
                                    depth=0,
                                )
                        if (
                            uncompressed_size is None
                            or compressed_size is None
                            or position <= 0
                        ):
                            raise ValueError
                        return position, uncompressed_size, compressed_size

                    def validate_page_bounds(spill_path: str) -> None:
                        validated_pages = 0
                        try:
                            source_size = os.path.getsize(spill_path)
                            with open(spill_path, "rb") as encoded_source:
                                for row_group_index in range(
                                    parquet_metadata.num_row_groups
                                ):
                                    row_group = parquet_metadata.row_group(
                                        row_group_index,
                                    )
                                    for column_index in range(
                                        row_group.num_columns
                                    ):
                                        column = row_group.column(column_index)
                                        offsets = []
                                        for raw_offset in (
                                            column.dictionary_page_offset,
                                            column.data_page_offset,
                                        ):
                                            if (
                                                type(raw_offset) is int
                                                and raw_offset >= 4
                                            ):
                                                offsets.append(raw_offset)
                                        compressed_chunk_bytes = int(
                                            column.total_compressed_size
                                        )
                                        if (
                                            not offsets
                                            or compressed_chunk_bytes <= 0
                                        ):
                                            raise ValueError
                                        page_offset = min(offsets)
                                        chunk_end = (
                                            page_offset + compressed_chunk_bytes
                                        )
                                        if chunk_end > source_size:
                                            raise ValueError
                                        while page_offset < chunk_end:
                                            validated_pages += 1
                                            if (
                                                validated_pages
                                                > _MAX_MIGRATION_PARQUET_PAGES
                                            ):
                                                raise ValueError
                                            encoded_source.seek(page_offset)
                                            header_bytes = encoded_source.read(
                                                min(
                                                    _MAX_MIGRATION_PAGE_HEADER_BYTES,
                                                    chunk_end - page_offset,
                                                )
                                            )
                                            (
                                                header_size,
                                                uncompressed_page_bytes,
                                                compressed_page_bytes,
                                            ) = decode_page_header(header_bytes)
                                            if (
                                                uncompressed_page_bytes < 0
                                                or compressed_page_bytes < 0
                                            ):
                                                raise ValueError
                                            if (
                                                uncompressed_page_bytes
                                                > _MAX_MIGRATION_ARRAY_PAGE_BYTES
                                                or compressed_page_bytes
                                                > _MAX_MIGRATION_ARRAY_PAGE_BYTES
                                            ):
                                                raise OverflowError
                                            next_page = (
                                                page_offset
                                                + header_size
                                                + compressed_page_bytes
                                            )
                                            if (
                                                next_page <= page_offset
                                                or next_page > chunk_end
                                            ):
                                                raise ValueError
                                            page_offset = next_page
                                        if page_offset != chunk_end:
                                            raise ValueError
                        except OverflowError:
                            raise ValueError(
                                f"Table {simple} fixed-size Array page exceeds "
                                "the migration decode limit"
                            ) from None
                        except (OSError, TypeError, ValueError):
                            raise RuntimeError(
                                f"Table {simple} fixed-size Array page layout "
                                "is invalid"
                            ) from None

                    download_to_file = getattr(
                        self.storage, "download_to_file", None,
                    )
                    if not callable(download_to_file):
                        raise RuntimeError(
                            "Offline v2.4 fixed-size Array migration requires "
                            "sealed streaming downloads"
                        )
                    spill_directory = tempfile.TemporaryDirectory(
                        prefix="supertable-v2-4-fixed-array-",
                    )
                    try:
                        os.chmod(spill_directory.name, 0o700)
                        spill_path = os.path.join(
                            spill_directory.name,
                            "source.parquet",
                        )
                        descriptor = os.open(
                            spill_path,
                            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                            0o600,
                        )
                    except BaseException:
                        spill_directory.cleanup()
                        raise
                    try:
                        with os.fdopen(descriptor, "wb") as spill:
                            written = download_to_file(
                                path,
                                spill,
                                expected=observed_before,
                                chunk_size=1024 * 1024,
                            )
                            spill.flush()
                        if (
                            type(written) is not int
                            or written != observed_before.size
                            or os.path.getsize(spill_path) != observed_before.size
                        ):
                            raise RuntimeError(
                                f"Table {simple} fixed-size Array resource "
                                "download was incomplete"
                            )
                        observed_download = self.storage.stat_object(path)
                        if (
                            not isinstance(observed_download, ObjectMetadata)
                            or observed_download != observed_before
                        ):
                            raise RuntimeError(
                                f"Table {simple} fixed-size Array resource "
                                "changed during download"
                            )
                        validate_page_bounds(spill_path)
                        import polars as pl

                        streamed_group = 0
                        streamed_group_rows = 0
                        pending_frames: list[Any] = []
                        pending_estimated_bytes = 0
                        pending_rows = 0
                        pending_group: Optional[int] = None

                        def materialize_pending() -> Optional[tuple[Any, Any]]:
                            nonlocal pending_frames
                            nonlocal pending_estimated_bytes
                            nonlocal pending_rows
                            nonlocal pending_group
                            if not pending_frames:
                                return None
                            proof_frame = (
                                pending_frames[0]
                                if len(pending_frames) == 1
                                else pl.concat(
                                    pending_frames,
                                    how="vertical",
                                    rechunk=True,
                                )
                            )
                            if (
                                proof_frame.height != pending_rows
                                or proof_frame.estimated_size() > 8 * 1024 * 1024
                            ):
                                raise RuntimeError(
                                    f"Table {simple} fixed-size Array decoder "
                                    "violated its bounded proof-batch contract"
                                )
                            arrow_projection = (
                                [
                                    str(field.name)
                                    for field in arrow_schema
                                    if not contains_fixed_size_list(field.type)
                                ]
                                if requires_polars_only_proof
                                else list(proof_frame.columns)
                            )
                            batch_table = proof_frame.select(
                                arrow_projection,
                            ).to_arrow()
                            if batch_table.nbytes > 8 * 1024 * 1024:
                                raise ValueError(
                                    f"Table {simple} fixed-size Array batch "
                                    "exceeds the migration decode limit"
                                )
                            result = (batch_table, proof_frame)
                            pending_frames = []
                            pending_estimated_bytes = 0
                            pending_rows = 0
                            pending_group = None
                            return result

                        def iter_isolated_polars_frames() -> Any:
                            batch_path = f"{spill_path}.proof.parquet"
                            temporary_batch_path = f"{batch_path}.tmp"
                            worker_script = r'''
import os
import resource
import sys

import polars as pl


class LogicalLimit(Exception):
    pass


def main():
    os.umask(0o077)
    source_path = sys.argv[1]
    batch_path = sys.argv[2]
    temporary_batch_path = sys.argv[3]
    source_chunk_rows = int(sys.argv[4])
    expected_rows = int(sys.argv[5])
    logical_limit = int(sys.argv[6])
    target_bytes = int(sys.argv[7])
    max_batch_rows = int(sys.argv[8])
    address_headroom = int(sys.argv[9])

    page_size = int(os.sysconf("SC_PAGE_SIZE"))
    with open("/proc/self/statm", "r", encoding="ascii") as status:
        baseline_address_bytes = int(status.read().split()[0]) * page_size
    source_size = os.path.getsize(source_path)
    desired_limit = baseline_address_bytes + source_size + address_headroom
    current_soft, current_hard = resource.getrlimit(resource.RLIMIT_AS)
    if current_hard != resource.RLIM_INFINITY:
        desired_limit = min(desired_limit, int(current_hard))
    if desired_limit <= baseline_address_bytes:
        raise RuntimeError
    resource.setrlimit(resource.RLIMIT_AS, (desired_limit, current_hard))

    pending = []
    pending_bytes = 0
    pending_rows = 0
    total_rows = 0

    def emit():
        nonlocal pending
        nonlocal pending_bytes
        nonlocal pending_rows
        if not pending:
            return
        frame = (
            pending[0]
            if len(pending) == 1
            else pl.concat(pending, how="vertical", rechunk=True)
        )
        estimated = int(frame.estimated_size())
        if (
            frame.height != pending_rows
            or estimated < 0
            or estimated > logical_limit
        ):
            raise LogicalLimit
        frame.write_parquet(
            temporary_batch_path,
            compression="zstd",
            statistics=False,
        )
        os.chmod(temporary_batch_path, 0o600)
        os.replace(temporary_batch_path, batch_path)
        print(f"BATCH\t{frame.height}\t{estimated}", flush=True)
        if sys.stdin.readline() != "NEXT\n":
            raise RuntimeError
        pending = []
        pending_bytes = 0
        pending_rows = 0

    scan = pl.scan_parquet(
        source_path,
        parallel="none",
        use_statistics=False,
        glob=False,
        rechunk=False,
        low_memory=True,
        cache=False,
    )
    for frame in scan.collect_batches(
        chunk_size=source_chunk_rows,
        engine="streaming",
    ):
        if (
            not isinstance(frame, pl.DataFrame)
            or not 1 <= frame.height <= source_chunk_rows
        ):
            raise RuntimeError
        estimated = int(frame.estimated_size())
        if estimated < 0 or estimated > logical_limit:
            raise LogicalLimit
        if pending and (
            pending_bytes + estimated > target_bytes
            or pending_rows + frame.height > max_batch_rows
        ):
            emit()
        pending.append(frame)
        pending_bytes += estimated
        pending_rows += frame.height
        total_rows += frame.height
        if pending_bytes >= target_bytes or pending_rows >= max_batch_rows:
            emit()
    emit()
    if total_rows != expected_rows:
        raise RuntimeError
    print(f"DONE\t{total_rows}", flush=True)


try:
    main()
except LogicalLimit:
    os._exit(42)
except MemoryError:
    os._exit(42)
except BaseException:
    os._exit(43)
'''
                            worker_environment = os.environ.copy()
                            worker_environment["POLARS_MAX_THREADS"] = "1"
                            worker_environment["PYTHONUNBUFFERED"] = "1"
                            worker_environment.pop("PYTHONINSPECT", None)
                            worker = subprocess.Popen(
                                [
                                    sys.executable,
                                    "-c",
                                    worker_script,
                                    spill_path,
                                    batch_path,
                                    temporary_batch_path,
                                    str(polars_scan_chunk_rows),
                                    str(declared_rows),
                                    str(8 * 1024 * 1024),
                                    str(2 * 1024 * 1024),
                                    "4096",
                                    str(
                                        _MAX_MIGRATION_ARRAY_WORKER_HEADROOM_BYTES
                                    ),
                                ],
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL,
                                text=True,
                                bufsize=1,
                                close_fds=True,
                                env=worker_environment,
                            )
                            worker_was_killed = False

                            def terminate_worker() -> None:
                                nonlocal worker_was_killed
                                if worker.poll() is None:
                                    worker_was_killed = True
                                    worker.terminate()
                                    try:
                                        worker.wait(timeout=2)
                                    except subprocess.TimeoutExpired:
                                        worker.kill()
                                        worker.wait(timeout=2)

                            def enforce_worker_memory() -> None:
                                if worker.poll() is not None:
                                    return
                                try:
                                    with open(
                                        f"/proc/{worker.pid}/status",
                                        "r",
                                        encoding="ascii",
                                    ) as worker_status:
                                        status_lines = worker_status.readlines()
                                except OSError:
                                    if worker.poll() is not None:
                                        return
                                    terminate_worker()
                                    raise RuntimeError(
                                        f"Table {simple} Array worker memory "
                                        "could not be bounded"
                                    ) from None
                                try:
                                    rss_values = [
                                        int(line.split()[1]) * 1024
                                        for line in status_lines
                                        if line.startswith("VmRSS:")
                                        and len(line.split()) >= 2
                                    ]
                                    peak_rss_values = [
                                        int(line.split()[1]) * 1024
                                        for line in status_lines
                                        if line.startswith("VmHWM:")
                                        and len(line.split()) >= 2
                                    ]
                                except (TypeError, ValueError):
                                    rss_values = []
                                    peak_rss_values = []
                                if (
                                    len(rss_values) != 1
                                    or len(peak_rss_values) != 1
                                ):
                                    if (
                                        worker.poll() is not None
                                        or any(
                                            line.startswith("State:")
                                            and "Z" in line.split()
                                            for line in status_lines
                                        )
                                    ):
                                        return
                                    terminate_worker()
                                    raise RuntimeError(
                                        f"Table {simple} Array worker memory "
                                        "could not be bounded"
                                    )
                                if (
                                    max(rss_values[0], peak_rss_values[0])
                                    > _MAX_MIGRATION_ARRAY_WORKER_RSS_BYTES
                                ):
                                    terminate_worker()
                                    raise ValueError(
                                        f"Table {simple} fixed-size Array "
                                        "logical batch exceeds the migration "
                                        "decode limit"
                                    ) from None

                            def worker_line() -> str:
                                if worker.stdout is None:
                                    raise RuntimeError
                                progress_deadline = (
                                    time.monotonic()
                                    + _MAX_MIGRATION_ARRAY_WORKER_STALL_SECONDS
                                )
                                while True:
                                    remaining = (
                                        progress_deadline - time.monotonic()
                                    )
                                    if remaining <= 0:
                                        terminate_worker()
                                        raise TimeoutError(
                                            f"Table {simple} Array worker stalled"
                                        ) from None
                                    enforce_worker_memory()
                                    ready, _, _ = select.select(
                                        [worker.stdout],
                                        [],
                                        [],
                                        min(0.05, remaining),
                                    )
                                    if ready:
                                        if time.monotonic() > progress_deadline:
                                            terminate_worker()
                                            raise TimeoutError(
                                                f"Table {simple} Array worker "
                                                "stalled"
                                            ) from None
                                        enforce_worker_memory()
                                        return worker.stdout.readline()

                            def raise_worker_failure() -> None:
                                return_code = worker.wait(timeout=2)
                                if (
                                    worker_was_killed
                                    or return_code == 42
                                    or return_code < 0
                                ):
                                    raise ValueError(
                                        f"Table {simple} fixed-size Array "
                                        "logical batch exceeds the migration "
                                        "decode limit"
                                    ) from None
                                raise RuntimeError(
                                    f"Table {simple} fixed-size Array isolated "
                                    "decoder failed"
                                ) from None

                            try:
                                while True:
                                    line = worker_line()
                                    if not line:
                                        raise_worker_failure()
                                    parts = line.rstrip("\n").split("\t")
                                    if len(parts) == 2 and parts[0] == "DONE":
                                        try:
                                            worker_rows = int(parts[1])
                                        except ValueError:
                                            raise RuntimeError from None
                                        if worker_rows != declared_rows:
                                            raise RuntimeError(
                                                f"Table {simple} fixed-size Array "
                                                "isolated scan is incomplete"
                                            )
                                        if worker.wait(timeout=2) != 0:
                                            raise_worker_failure()
                                        break
                                    if len(parts) != 3 or parts[0] != "BATCH":
                                        terminate_worker()
                                        raise RuntimeError(
                                            f"Table {simple} fixed-size Array "
                                            "isolated decoder protocol failed"
                                        )
                                    try:
                                        worker_rows = int(parts[1])
                                        worker_bytes = int(parts[2])
                                    except ValueError:
                                        terminate_worker()
                                        raise RuntimeError(
                                            f"Table {simple} fixed-size Array "
                                            "isolated decoder protocol failed"
                                        ) from None
                                    if (
                                        not 1 <= worker_rows <= 4096
                                        or not 0 <= worker_bytes
                                        <= 8 * 1024 * 1024
                                    ):
                                        terminate_worker()
                                        raise RuntimeError(
                                            f"Table {simple} fixed-size Array "
                                            "isolated batch contract failed"
                                        )
                                    try:
                                        batch_file_size = os.path.getsize(batch_path)
                                        batch_mode = os.stat(
                                            batch_path,
                                            follow_symlinks=False,
                                        ).st_mode
                                    except OSError:
                                        terminate_worker()
                                        raise RuntimeError(
                                            f"Table {simple} fixed-size Array "
                                            "isolated batch contract failed"
                                        ) from None
                                    if (
                                        not 0 < batch_file_size
                                        <= 16 * 1024 * 1024
                                        or not stat.S_ISREG(batch_mode)
                                        or batch_mode & 0o077
                                    ):
                                        terminate_worker()
                                        raise RuntimeError(
                                            f"Table {simple} fixed-size Array "
                                            "isolated batch contract failed"
                                        )
                                    try:
                                        frame = pl.read_parquet(
                                            batch_path,
                                            parallel="none",
                                            use_statistics=False,
                                            glob=False,
                                            rechunk=False,
                                            low_memory=True,
                                            memory_map=False,
                                        )
                                    except (KeyboardInterrupt, SystemExit):
                                        raise
                                    except BaseException:
                                        terminate_worker()
                                        raise RuntimeError(
                                            f"Table {simple} fixed-size Array "
                                            "isolated batch could not be read"
                                        ) from None
                                    if (
                                        not isinstance(frame, pl.DataFrame)
                                        or frame.height != worker_rows
                                        or frame.estimated_size()
                                        > 8 * 1024 * 1024
                                        or frame.columns
                                        != [str(field.name) for field in arrow_schema]
                                    ):
                                        terminate_worker()
                                        raise RuntimeError(
                                            f"Table {simple} fixed-size Array "
                                            "isolated batch contract failed"
                                        )
                                    yield frame
                                    os.remove(batch_path)
                                    if worker.stdin is None:
                                        raise RuntimeError
                                    try:
                                        worker.stdin.write("NEXT\n")
                                        worker.stdin.flush()
                                    except (BrokenPipeError, OSError):
                                        raise_worker_failure()
                            finally:
                                terminate_worker()
                                for temporary_path in (
                                    batch_path,
                                    temporary_batch_path,
                                ):
                                    try:
                                        os.remove(temporary_path)
                                    except FileNotFoundError:
                                        pass
                                if worker.stdin is not None:
                                    worker.stdin.close()
                                if worker.stdout is not None:
                                    worker.stdout.close()

                        for frame in iter_isolated_polars_frames():
                            if (
                                not isinstance(frame, pl.DataFrame)
                                or not 1 <= frame.height <= 4096
                                or frame.estimated_size() > 8 * 1024 * 1024
                            ):
                                raise RuntimeError(
                                    f"Table {simple} fixed-size Array decoder "
                                    "violated its bounded batch contract"
                                )
                            frame_offset = 0
                            while frame_offset < frame.height:
                                while (
                                    streamed_group < len(row_group_sizes)
                                    and row_group_sizes[streamed_group] == 0
                                ):
                                    streamed_group += 1
                                if streamed_group >= len(row_group_sizes):
                                    raise RuntimeError(
                                        f"Table {simple} fixed-size Array scan "
                                        "overflowed its row groups"
                                    )
                                remaining = (
                                    row_group_sizes[streamed_group]
                                    - streamed_group_rows
                                )
                                if (
                                    pending_group is not None
                                    and pending_group != streamed_group
                                ):
                                    materialized = materialize_pending()
                                    if materialized is not None:
                                        yield materialized
                                if pending_rows == 4096:
                                    materialized = materialize_pending()
                                    if materialized is not None:
                                        yield materialized
                                take = min(
                                    remaining,
                                    frame.height - frame_offset,
                                    4096 - pending_rows,
                                )
                                proof_piece = frame.slice(frame_offset, take)
                                piece_estimated_bytes = int(
                                    proof_piece.estimated_size()
                                )
                                if (
                                    pending_frames
                                    and pending_estimated_bytes
                                    + piece_estimated_bytes
                                    > 2 * 1024 * 1024
                                ):
                                    materialized = materialize_pending()
                                    if materialized is not None:
                                        yield materialized
                                    continue
                                pending_frames.append(proof_piece)
                                pending_estimated_bytes += piece_estimated_bytes
                                pending_rows += take
                                pending_group = streamed_group
                                frame_offset += take
                                streamed_group_rows += take
                                if (
                                    streamed_group_rows
                                    == row_group_sizes[streamed_group]
                                ):
                                    materialized = materialize_pending()
                                    if materialized is not None:
                                        yield materialized
                                    streamed_group += 1
                                    streamed_group_rows = 0
                                elif (
                                    pending_rows == 4096
                                    or pending_estimated_bytes
                                    >= 2 * 1024 * 1024
                                ):
                                    materialized = materialize_pending()
                                    if materialized is not None:
                                        yield materialized
                        materialized = materialize_pending()
                        if materialized is not None:
                            # Keep the native Polars frame beside its Arrow
                            # projection. Reconstructing nullable nested Array
                            # slices from Arrow can panic in Polars.
                            yield materialized
                    finally:
                        spill_directory.cleanup()

                raw_batches = (
                    iter_polars_fixed_size_batches()
                    if requires_polars_scan
                    else iter_batches(
                        path,
                        max_decoded_bytes=8 * 1024 * 1024,
                        columns=None,
                    )
                )
                active_batches = raw_batches
                for raw_batch in raw_batches:
                    polars_batch = None
                    if (
                        isinstance(raw_batch, tuple)
                        and len(raw_batch) == 2
                    ):
                        raw_batch, polars_batch = raw_batch
                    if isinstance(raw_batch, pa.RecordBatch):
                        table = pa.Table.from_batches([raw_batch])
                    elif isinstance(raw_batch, pa.Table):
                        table = raw_batch
                    else:
                        raise RuntimeError(
                            "Bounded migration scan returned an invalid Arrow batch"
                        )
                    table.validate(full=True)
                    if table.nbytes > 8 * 1024 * 1024:
                        raise ValueError(
                            f"Table {simple} row exceeds the migration decode limit"
                        )
                    try:
                        rowids = table.column("__rowid__")
                        timestamps = table.column("__timestamp__")
                    except KeyError:
                        raise ValueError(
                            f"Table {simple} v2.4 data pages lack system columns"
                        ) from None
                    if (
                        rowids.type != pa.int64()
                        or timestamps.type != expected_timestamp_type
                        or rowids.null_count
                        or timestamps.null_count
                    ):
                        raise ValueError(
                            f"Table {simple} v2.4 data pages have invalid system values"
                        )
                    for column_name in resource_variable_bounds:
                        try:
                            observed_maximum = pc.max(
                                pc.binary_length(table.column(column_name)),
                            ).as_py()
                        except Exception:
                            raise ValueError(
                                f"Table {simple} cannot seal v2.4 variable-width values"
                            ) from None
                        if observed_maximum is not None:
                            resource_variable_bounds[column_name] = max(
                                resource_variable_bounds[column_name],
                                int(observed_maximum),
                            )
                    scanned_rows += int(table.num_rows)
                    table_offset = 0
                    while table_offset < table.num_rows:
                        if current_group >= len(row_group_sizes):
                            raise RuntimeError(
                                f"Table {simple} v2.4 row-group scan overflowed"
                            )
                        remaining = (
                            row_group_sizes[current_group] - current_group_rows
                        )
                        take = min(remaining, int(table.num_rows) - table_offset)
                        consume_group_slice(
                            current_group,
                            table.slice(table_offset, take),
                            (
                                polars_batch.slice(table_offset, take)
                                if polars_batch is not None
                                else None
                            ),
                        )
                        table_offset += take
                        current_group_rows += take
                        if current_group_rows == row_group_sizes[current_group]:
                            finish_row_group(current_group)
                            current_group += 1
                            current_group_rows = 0
                            group_accumulators = {}
                            while (
                                current_group < len(row_group_sizes)
                                and row_group_sizes[current_group] == 0
                            ):
                                finish_row_group(current_group)
                                current_group += 1
                    values = rowids.combine_chunks().to_numpy(
                        zero_copy_only=False,
                    )
                    if len(values) > 0:
                        chunk_min = int(values.min())
                        chunk_max = int(values.max())
                        if chunk_min <= 0 or chunk_max > MAX_TABLE_ROWID:
                            raise ValueError(
                                f"Table {simple} contains invalid v2.4 row IDs"
                            )
                        minimum = (
                            chunk_min if minimum is None
                            else min(minimum, chunk_min)
                        )
                        maximum = (
                            chunk_max if maximum is None
                            else max(maximum, chunk_max)
                        )
                        digest.update(
                            values.astype(">i8", copy=False).tobytes(order="C")
                        )
                        try:
                            connection.executemany(
                                "INSERT INTO rowids(value, resource_id) "
                                "VALUES (?, ?)",
                                (
                                    (int(value), resource_id)
                                    for value in values
                                ),
                            )
                            connection.commit()
                        except sqlite3.IntegrityError:
                            raise ValueError(
                                f"Table {simple} reuses a v2.4 row ID"
                            ) from None

                close_batches = getattr(active_batches, "close", None)
                if callable(close_batches):
                    close_batches()
                active_batches = None

                if scanned_rows != declared_rows:
                    raise ValueError(
                        f"Table {simple} v2.4 data row count changed during scan"
                    )
                if (
                    current_group != len(row_group_sizes)
                    or current_group_rows != 0
                ):
                    raise RuntimeError(
                        f"Table {simple} v2.4 row-group scan is incomplete"
                    )
                observed_after = self.storage.stat_object(path)
                if (
                    not isinstance(observed_after, ObjectMetadata)
                    or observed_after != observed_before
                    or _object_seal_document(observed_after)
                    != resource.get("object_seal")
                ):
                    raise RuntimeError(
                        f"Table {simple} data resource changed during migration"
                    )
                high_watermark = max(high_watermark, maximum or 0)
                facts[path] = {
                    "version": 1,
                    "rows": declared_rows,
                    "nonnull": declared_rows,
                    "unique": declared_rows,
                    "minimum": minimum,
                    "maximum": maximum,
                    "digest": digest.hexdigest(),
                    "footer_sha256": resource["footer_sha256"],
                }
                variable_value_bounds[path] = resource_variable_bounds
            return (
                directory,
                connection,
                facts,
                high_watermark,
                variable_value_bounds,
            )
        except BaseException:
            close_batches = getattr(active_batches, "close", None)
            if callable(close_batches):
                try:
                    close_batches()
                except BaseException:
                    pass
            connection.close()
            directory.cleanup()
            raise

    def _read_v2_4_rowid_sequence(self, *, simple: str) -> int:
        """Read the exact v2.4 allocator floor without changing Redis."""
        from supertable import redis_keys as RK
        from supertable.tombstone_manifest_v2 import MAX_JSON_EXACT_INTEGER

        redis_client = getattr(self.catalog, "r", None)
        if redis_client is None or not callable(getattr(redis_client, "get", None)):
            raise RuntimeError(
                "Offline v2.4 migration requires direct Redis sequence reads"
            )
        raw = redis_client.get(
            RK.meta_rowid_seq(self.organization, self.super_name, simple),
        )
        if raw is None:
            return 0
        if isinstance(raw, bytes):
            try:
                encoded = raw.decode("ascii")
            except UnicodeDecodeError:
                raise ValueError(
                    f"Table {simple} has an invalid v2.4 row-ID allocator"
                ) from None
        elif isinstance(raw, str):
            encoded = raw
        else:
            raise ValueError(
                f"Table {simple} has an invalid v2.4 row-ID allocator"
            )
        if not encoded or any(character < "0" or character > "9" for character in encoded):
            raise ValueError(
                f"Table {simple} has an invalid v2.4 row-ID allocator"
            )
        normalized = encoded.lstrip("0") or "0"
        maximum_text = str(MAX_JSON_EXACT_INTEGER)
        if (
            len(normalized) > len(maximum_text)
            or (
                len(normalized) == len(maximum_text)
                and normalized > maximum_text
            )
        ):
            raise ValueError(
                f"Table {simple} v2.4 row-ID allocator exceeds the current "
                "Redis snapshot limit"
            )
        return int(normalized)

    @staticmethod
    def _legacy_metadata_migration_required(
        *,
        simple: str,
        table_dir: str,
        version: int,
        snapshot: Dict[str, Any],
    ) -> bool:
        """Return whether an unmarked snapshot lacks current metadata seals."""
        from supertable.processing import (
            resource_object_seal,
            resource_stats_seal,
        )
        from supertable.row_identity import snapshot_proves_stable_rowids
        from supertable.simple_table import _contained_artifact_path
        from supertable.tombstone_manifest_v2 import (
            normalize_snapshot_tombstone_state,
        )

        for field_name, expected in (
            ("simple_name", simple),
            ("location", table_dir),
            ("snapshot_version", version),
        ):
            if field_name in snapshot and snapshot.get(field_name) != expected:
                raise ValueError(
                    f"Table {simple} snapshot {field_name} disagrees with its leaf"
                )
        migration_version = snapshot.get("_legacy_metadata_migration_version")
        if migration_version is not None:
            if (
                type(migration_version) is not int
                or migration_version < 1
                or migration_version > _LEGACY_METADATA_MIGRATION_VERSION
            ):
                raise ValueError(
                    f"Table {simple} has an invalid metadata migration version"
                )

        required_snapshot_fields = {
            "simple_name",
            "location",
            "snapshot_version",
            "last_updated_ms",
            "previous_snapshot",
            "schema",
            "resources",
            "tombstone",
            "tombstone_rows",
            "tombstone_digest",
            "stats_file",
            "stats_rows",
            "rowid_high_watermark",
            "_row_filter",
        }
        # A marker is only a version discriminator; it is never proof that the
        # publication is complete.  Every marked successor must carry the
        # exact stable-row-ID and row-filter fields written by this migration.
        if not required_snapshot_fields.issubset(snapshot):
            if migration_version is not None:
                raise ValueError(
                    f"Table {simple} marked snapshot lacks required current fields"
                )
            return True
        last_updated_ms = snapshot.get("last_updated_ms")
        stats_rows = snapshot.get("stats_rows")
        if (
            type(last_updated_ms) is not int
            or last_updated_ms < 0
            or type(stats_rows) is not int
            or stats_rows < 0
        ):
            raise ValueError(
                f"Table {simple} has invalid current snapshot counters"
            )
        normalize_snapshot_tombstone_state(snapshot)
        stats_file = snapshot.get("stats_file")
        if stats_file is None:
            if stats_rows != 0:
                raise ValueError(
                    f"Table {simple} has statistics rows without an artifact"
                )
        else:
            try:
                _contained_artifact_path(
                    stats_file,
                    label="current migration statistics",
                    required_prefix=os.path.join(table_dir, "stats"),
                )
            except (TypeError, ValueError):
                raise ValueError(
                    f"Table {simple} has an invalid statistics artifact"
                ) from None
        if migration_version is not None:
            if not snapshot_proves_stable_rowids(snapshot):
                raise ValueError(
                    f"Table {simple} marked snapshot has an invalid stable "
                    "row-ID proof"
                )
            return migration_version != _LEGACY_METADATA_MIGRATION_VERSION
        if not snapshot_proves_stable_rowids(snapshot):
            return True
        resources = snapshot.get("resources")
        if not isinstance(resources, list):  # guarded by row-ID proof
            return True
        seen_paths: set[str] = set()
        data_dir = os.path.join(table_dir, "data")
        for resource in resources:
            if not isinstance(resource, dict):
                return True
            try:
                resource_path = _contained_artifact_path(
                    resource.get("file"),
                    label="current migration resource",
                    required_prefix=data_dir,
                )
            except (TypeError, ValueError):
                return True
            if resource_path in seen_paths:
                return True
            seen_paths.add(resource_path)
            for field_name, allow_zero in (
                ("file_size", False),
                ("rows", True),
                ("columns", True),
            ):
                value = resource.get(field_name)
                if (
                    type(value) is not int
                    or value < 0
                    or (not allow_zero and value == 0)
                ):
                    return True
            if resource_stats_seal(resource) is None:
                return True
            if (
                "object_seal" in resource
                and resource["object_seal"] is not None
                and resource_object_seal(resource) is None
            ):
                return True
        return False

    def _migrate_legacy_tombstone(
        self,
        *,
        simple: str,
        snapshot: Dict[str, Any],
        version: int,
        allowed_files: set[str],
        available_rows: int,
        publish_successor: bool = True,
        legacy_rowid_index: Any = None,
    ) -> None:
        """Validate a v1/v2/v3 deletion vector and bind it to the successor."""
        import polars as pl

        from supertable.processing import (
            load_tombstone,
            load_tombstone_manifest_from_storage,
            persist_tombstone_manifest_v2,
            persist_tombstone_v3_frame,
            validate_tombstone_frame,
        )
        from supertable.simple_table import (
            _MAX_RESTORE_AGGREGATE_FOOTER_BYTES,
            _MAX_RESTORE_TOMBSTONE_BYTES,
            _MAX_RESTORE_TOMBSTONE_DECODED_BYTES,
            _MAX_RESTORE_TOMBSTONE_ROWS,
            _bounded_restored_tombstone_frame,
            _contained_artifact_path,
            _sealed_parquet_metadata,
            _validate_physical_containment,
        )
        from supertable.storage.storage_interface import ObjectMetadata
        from supertable.tombstone_manifest_v2 import (
            NormalizedSnapshotTombstoneState,
            TOMBSTONE_FORMAT_V1,
            TOMBSTONE_FORMAT_V2,
            TOMBSTONE_FORMAT_V3,
            normalize_snapshot_tombstone_state,
        )

        tombstone_shape = {
            field
            for field in (
                "tombstone",
                "tombstone_rows",
                "tombstone_digest",
                "tombstone_format",
                "tombstone_object_seal",
            )
            if field in snapshot
        }
        legacy_v2_4_active = bool(
            tombstone_shape == {"tombstone", "tombstone_rows"}
            and isinstance(snapshot.get("tombstone"), str)
            and snapshot["tombstone"].endswith(".parquet")
            and type(snapshot.get("tombstone_rows")) is int
            and snapshot["tombstone_rows"] > 0
        )
        legacy_v2_4_empty = bool(
            tombstone_shape == {"tombstone", "tombstone_rows"}
            and snapshot.get("tombstone") is None
            and type(snapshot.get("tombstone_rows")) is int
            and snapshot["tombstone_rows"] == 0
        )
        if legacy_v2_4_active:
            # v2.4 predates snapshot-level deletion-vector seals.  This exact
            # historical shape is admitted only by the offline migration; all
            # ordinary readers and publishers remain strict.
            state = NormalizedSnapshotTombstoneState(
                pointer=snapshot["tombstone"],
                rows=snapshot["tombstone_rows"],
                digest=None,
                tombstone_format=TOMBSTONE_FORMAT_V1,
                format_present=False,
            )
        else:
            state = normalize_snapshot_tombstone_state(snapshot)
        if state.pointer is None:
            snapshot["tombstone"] = None
            snapshot["tombstone_rows"] = 0
            snapshot["tombstone_digest"] = None
            if legacy_v2_4_empty and publish_successor:
                # Pin the new immutable-vector logic even before the table has
                # deletions; current writers keep an explicit v3 state sticky.
                snapshot["tombstone_format"] = TOMBSTONE_FORMAT_V3
            elif state.format_present:
                snapshot["tombstone_format"] = state.tombstone_format
            else:
                snapshot.pop("tombstone_format", None)
            snapshot.pop("tombstone_object_seal", None)
            return
        if (
            type(available_rows) is not int
            or available_rows < 0
            or state.rows > available_rows
            or state.rows > _MAX_RESTORE_TOMBSTONE_ROWS
        ):
            raise ValueError(
                f"Table {simple} deletion-vector row count exceeds its safety bound"
            )

        tombstone_dir = os.path.join(
            self.organization, self.super_name, "tables", simple, "tombstone",
        )
        pointer = _contained_artifact_path(
            state.pointer,
            label="migration tombstone",
            required_prefix=tombstone_dir,
        )
        _validate_physical_containment(
            self.storage, pointer, tombstone_dir,
        )

        def bounded_frame(
            artifact_path: str,
            *,
            expected_rows: int,
            expected_digest: Optional[str],
            tombstone_format: int,
            expected_size: Optional[int],
            compressed_budget: int,
            decoded_budget: int,
        ) -> tuple[Any, int, int, int]:
            observed_before = self.storage.stat_object(artifact_path)
            if (
                not isinstance(observed_before, ObjectMetadata)
                or type(observed_before.size) is not int
                or observed_before.size <= 0
                or observed_before.size > compressed_budget
                or not observed_before.identity_token()
                or (
                    expected_size is not None
                    and observed_before.size != expected_size
                )
            ):
                raise ValueError(
                    f"Table {simple} deletion-vector object exceeds its safety bound"
                )
            observed, parquet_metadata, footer_bytes = _sealed_parquet_metadata(
                self.storage,
                artifact_path,
                expected_size=observed_before.size,
            )
            if observed != observed_before or int(parquet_metadata.num_rows) != (
                expected_rows
            ):
                raise ValueError(
                    f"Table {simple} deletion-vector row-count seal is invalid"
                )
            expanded_bytes = 0
            for group_index in range(int(parquet_metadata.num_row_groups)):
                group = parquet_metadata.row_group(group_index)
                for column_index in range(int(group.num_columns)):
                    value = int(
                        group.column(column_index).total_uncompressed_size or 0
                    )
                    if value < 0:
                        raise ValueError(
                            f"Table {simple} deletion-vector metadata is invalid"
                        )
                    expanded_bytes += value
                    if expanded_bytes > decoded_budget:
                        raise ValueError(
                            f"Table {simple} deletion vector exceeds its decoded-byte limit"
                        )
            frame = _bounded_restored_tombstone_frame(
                self.storage,
                artifact_path,
                observed=observed,
                expected_rows=expected_rows,
                expected_digest=expected_digest,
                tombstone_format=tombstone_format,
                allowed_files=allowed_files,
            )
            return frame, observed.size, footer_bytes, expanded_bytes

        validated_frame: Any
        manifest = None
        if state.tombstone_format == TOMBSTONE_FORMAT_V2:
            manifest = load_tombstone_manifest_from_storage(
                self.storage,
                pointer,
                expected_organization=self.organization,
                expected_super_name=self.super_name,
                expected_simple_name=simple,
                pinned_snapshot_version=version,
                expected_total_rows=state.rows,
                expected_digest=state.digest,
                expected_segment_prefix=tombstone_dir + "/",
            )
            frames = []
            compressed_bytes = 0
            decoded_bytes = 0
            footer_bytes = 0
            for segment in manifest.segments:
                segment_path = _contained_artifact_path(
                    segment.file,
                    label="migration tombstone segment",
                    required_prefix=tombstone_dir,
                )
                _validate_physical_containment(
                    self.storage, segment_path, tombstone_dir,
                )
                frame, object_bytes, segment_footer, segment_decoded = bounded_frame(
                    segment_path,
                    expected_rows=segment.rows,
                    expected_digest=segment.digest,
                    tombstone_format=TOMBSTONE_FORMAT_V1,
                    expected_size=segment.file_size,
                    compressed_budget=(
                        _MAX_RESTORE_TOMBSTONE_BYTES - compressed_bytes
                    ),
                    decoded_budget=(
                        _MAX_RESTORE_TOMBSTONE_DECODED_BYTES - decoded_bytes
                    ),
                )
                compressed_bytes += object_bytes
                decoded_bytes += segment_decoded
                footer_bytes += segment_footer
                if footer_bytes > _MAX_RESTORE_AGGREGATE_FOOTER_BYTES:
                    raise ValueError(
                        f"Table {simple} deletion-vector footers exceed their safety bound"
                    )
                frames.append(frame)
            validated_frame = pl.concat(frames, how="vertical", rechunk=False)
            validated_frame = validate_tombstone_frame(
                validated_frame,
                expected_rows=state.rows,
                allowed_files=allowed_files,
                source="migration deletion-vector manifest union",
            )
        else:
            validated_frame, _bytes, _footer, _decoded = bounded_frame(
                pointer,
                expected_rows=state.rows,
                expected_digest=state.digest,
                tombstone_format=state.tombstone_format,
                expected_size=None,
                compressed_budget=_MAX_RESTORE_TOMBSTONE_BYTES,
                decoded_budget=_MAX_RESTORE_TOMBSTONE_DECODED_BYTES,
            )
        if validated_frame.height != state.rows:
            raise RuntimeError(
                f"Table {simple} deletion vector could not be validated"
            )
        if legacy_v2_4_active:
            if legacy_rowid_index is None:
                raise RuntimeError(
                    f"Table {simple} v2.4 deletion vector has no physical row index"
                )
            resource_ids = {
                str(file_path): int(resource_id)
                for file_path, resource_id in legacy_rowid_index.execute(
                    "SELECT file, id FROM resources"
                )
            }
            legacy_rowid_index.execute(
                "CREATE TEMP TABLE migration_tombstones ("
                "value INTEGER PRIMARY KEY, resource_id INTEGER NOT NULL"
                ") WITHOUT ROWID"
            )
            try:
                def indexed_tombstones():
                    for file_path, rowid in validated_frame.iter_rows():
                        resource_id = resource_ids.get(str(file_path))
                        if resource_id is None:
                            raise ValueError(
                                f"Table {simple} v2.4 deletion-vector entry "
                                "references an unknown resource"
                            )
                        yield int(rowid), resource_id

                legacy_rowid_index.executemany(
                    "INSERT INTO migration_tombstones(value, resource_id) "
                    "VALUES (?, ?)",
                    indexed_tombstones(),
                )
                mismatch = legacy_rowid_index.execute(
                    "SELECT 1 FROM migration_tombstones AS tombstone "
                    "LEFT JOIN rowids AS physical "
                    "ON physical.value = tombstone.value "
                    "AND physical.resource_id = tombstone.resource_id "
                    "WHERE physical.value IS NULL LIMIT 1"
                ).fetchone()
                if mismatch is not None:
                    raise ValueError(
                        f"Table {simple} v2.4 deletion-vector entry does not "
                        "identify a physical row"
                    )
            finally:
                legacy_rowid_index.execute("DROP TABLE migration_tombstones")
        if not publish_successor:
            return
        if legacy_v2_4_active:
            new_pointer, converted_frame, converted_state = (
                persist_tombstone_v3_frame(
                    tombstone_dir,
                    validated_frame,
                    3,
                    storage=self.storage,
                )
            )
            if (
                new_pointer is None
                or converted_frame.height != state.rows
                or converted_state.root_digest is None
            ):
                raise RuntimeError(
                    f"Table {simple} v2.4 deletion vector conversion failed"
                )
            try:
                reloaded_frame = load_tombstone(
                    new_pointer,
                    allow_cache=False,
                    required=True,
                    expected_rows=converted_frame.height,
                    expected_digest=converted_state.root_digest,
                    allowed_files=allowed_files,
                    tombstone_format=TOMBSTONE_FORMAT_V3,
                    storage=self.storage,
                )
            except Exception:
                raise RuntimeError(
                    f"Table {simple} generated deletion vector failed readback"
                ) from None
            if (
                reloaded_frame is None
                or not reloaded_frame.equals(converted_frame)
            ):
                raise RuntimeError(
                    f"Table {simple} generated deletion vector failed readback"
                )
            snapshot["tombstone"] = new_pointer
            snapshot["tombstone_rows"] = converted_frame.height
            snapshot["tombstone_digest"] = converted_state.root_digest
            snapshot["tombstone_format"] = TOMBSTONE_FORMAT_V3
        elif state.tombstone_format == TOMBSTONE_FORMAT_V2:
            if manifest is None:  # pragma: no cover - guarded by format branch
                raise RuntimeError("Validated v2 deletion vector has no manifest")
            new_pointer, successor_manifest = persist_tombstone_manifest_v2(
                tombstone_dir,
                organization=self.organization,
                super_name=self.super_name,
                simple_name=simple,
                base_snapshot_version=version,
                snapshot_version=version + 1,
                segments=manifest.segments,
                storage=self.storage,
            )
            snapshot["tombstone"] = new_pointer
            snapshot["tombstone_rows"] = successor_manifest.total_rows
            snapshot["tombstone_digest"] = successor_manifest.digest()
            snapshot["tombstone_format"] = TOMBSTONE_FORMAT_V2
        else:
            snapshot["tombstone"] = pointer
            snapshot["tombstone_rows"] = state.rows
            snapshot["tombstone_digest"] = state.digest
            if state.format_present or state.tombstone_format != TOMBSTONE_FORMAT_V1:
                snapshot["tombstone_format"] = state.tombstone_format
            else:
                snapshot.pop("tombstone_format", None)
        snapshot.pop("tombstone_object_seal", None)

    def _validate_legacy_resources(
        self,
        *,
        simple: str,
        snapshot: Dict[str, Any],
        allow_v2_4_timestamp_only: bool = False,
    ) -> tuple[list[Dict[str, Any]], dict[str, Any], Dict[str, str]]:
        """Validate and canonically reseal every legacy Parquet resource."""
        import pyarrow as pa

        from supertable.processing import stats_seal_for_metadata
        from supertable.simple_table import (
            _contained_artifact_path,
            _lossless_restore_physical_type,
            _object_seal_document,
            _polars_dtype_for_arrow_field,
            _restored_schema_field_names,
            _restored_schema_type_values,
            _sealed_parquet_metadata,
            _validate_declared_object_seal,
            _validate_physical_containment,
        )

        def legacy_polars_dtype_for_field(field: Any) -> Any:
            if not pa.types.is_dictionary(field.type):
                return _polars_dtype_for_arrow_field(field)
            try:
                import polars as pl

                dictionary = pa.DictionaryArray.from_arrays(
                    pa.array([], type=field.type.index_type),
                    pa.array([], type=field.type.value_type),
                    ordered=bool(field.type.ordered),
                )
                empty = pa.Table.from_arrays(
                    [dictionary],
                    schema=pa.schema([field]),
                )
                return pl.from_arrow(empty).schema[field.name]
            except Exception:
                raise ValueError(
                    "Restored column has unsupported logical metadata"
                ) from None

        resources = snapshot.get("resources")
        schema = snapshot.get("schema")
        if not isinstance(resources, list) or len(resources) > 10_000:
            raise ValueError(f"Table {simple} resource fan-out is invalid")
        if not isinstance(schema, (dict, list)):
            raise ValueError(f"Table {simple} schema is invalid")
        try:
            schema_size = len(json.dumps(schema, allow_nan=False).encode("utf-8"))
        except (TypeError, ValueError, UnicodeEncodeError):
            raise ValueError(f"Table {simple} schema is invalid") from None
        if schema_size > 1024 * 1024:
            raise ValueError(f"Table {simple} schema exceeds its size limit")

        # The v2.4 writer derived ``schema`` after injecting its physical
        # writer-owned columns.  A non-empty write injected both; an empty
        # write injected timestamp without reserving row IDs.  Accept that
        # timestamp-only shape only after the caller recognized the complete
        # authoritative v2.4 snapshot envelope.  A case alias or every other
        # partial reserved-field shape remains invalid.
        schema_for_public_validation = schema
        if isinstance(schema, dict):
            reserved_system_names = {
                name
                for name in schema
                if isinstance(name, str)
                and name.casefold() in {"__rowid__", "__timestamp__"}
            }
            if reserved_system_names:
                valid_reserved_system_names = {
                    "__rowid__", "__timestamp__",
                }
                if (
                    reserved_system_names != valid_reserved_system_names
                    and not (
                        allow_v2_4_timestamp_only
                        and reserved_system_names == {"__timestamp__"}
                    )
                ):
                    raise ValueError(f"Table {simple} schema is invalid")
                schema_for_public_validation = {
                    name: value
                    for name, value in schema.items()
                    if name not in {"__rowid__", "__timestamp__"}
                }

        declared_schema_names = _restored_schema_field_names(
            schema_for_public_validation,
        )
        data_dir = os.path.join(
            self.organization, self.super_name, "tables", simple, "data",
        )
        seen_resources: set[str] = set()
        physical_schema_folded: dict[str, str] = {}
        physical_schema_fields: dict[str, Any] = {}
        physical_schema_versions: dict[str, list[Any]] = {}
        footer_cache: dict[str, Any] = {}
        canonical_resources: list[Dict[str, Any]] = []
        total_rows = 0
        total_bytes = 0
        total_column_chunks = 0
        total_footer_bytes = 0

        for resource in resources:
            if not isinstance(resource, dict):
                raise ValueError(f"Table {simple} contains an invalid resource")
            resource_path = _contained_artifact_path(
                resource.get("file"),
                label="migration resource",
                required_prefix=data_dir,
            )
            if resource_path in seen_resources:
                raise ValueError(f"Table {simple} repeats a data resource")
            seen_resources.add(resource_path)
            _validate_physical_containment(
                self.storage, resource_path, data_dir,
            )
            try:
                object_metadata, parquet_metadata, footer_bytes = (
                    _sealed_parquet_metadata(
                        self.storage,
                        resource_path,
                        expected_size=None,
                    )
                )
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"Missing data resource for {simple}: {resource_path}"
                ) from None

            rows = int(parquet_metadata.num_rows)
            file_size = int(object_metadata.size)
            arrow_schema = parquet_metadata.schema.to_arrow_schema()
            columns = len(arrow_schema)
            physical_columns = int(parquet_metadata.num_columns)
            for field_name, declared, actual in (
                ("rows", resource.get("rows"), rows),
                ("file_size", resource.get("file_size"), file_size),
                ("columns", resource.get("columns"), columns),
            ):
                if field_name in resource and (
                    type(declared) is not int or declared != actual
                ):
                    raise RuntimeError(
                        f"Table {simple} resource {field_name} disagrees with Parquet"
                    )
            if rows < 0 or rows > 1_000_000_000 or file_size <= 0:
                raise ValueError(f"Table {simple} resource bounds are invalid")
            total_rows += rows
            total_bytes += file_size
            total_footer_bytes += footer_bytes
            total_column_chunks += (
                max(1, int(parquet_metadata.num_row_groups))
                * physical_columns
            )
            if total_rows > 1_000_000_000 or total_bytes > 2 * 1024 ** 4:
                raise ValueError(f"Table {simple} aggregate resource bounds are invalid")
            if total_footer_bytes > 256 * 1024 * 1024:
                raise ValueError(f"Table {simple} footer bytes exceed the safety limit")
            if total_column_chunks > _MAX_MIGRATION_COLUMN_CHUNKS:
                raise ValueError(f"Table {simple} footer fan-out exceeds the safety limit")

            footer_names: set[str] = set()
            for field in arrow_schema:
                column_name = str(field.name)
                folded_name = column_name.casefold()
                if folded_name in footer_names:
                    raise ValueError(
                        f"Table {simple} physical schema repeats a column"
                    )
                footer_names.add(folded_name)
                prior_name = physical_schema_folded.get(folded_name)
                if prior_name is not None and prior_name != column_name:
                    raise ValueError(
                        f"Table {simple} contains case-colliding columns"
                    )
                physical_schema_folded[folded_name] = column_name
                if folded_name in {"__rowid__", "__timestamp__"}:
                    if column_name != folded_name:
                        raise ValueError(
                            f"Table {simple} contains a noncanonical reserved "
                            "physical column"
                        )
                    continue
                if (
                    folded_name == "__file__"
                    or folded_name.startswith("__supertable_")
                ):
                    raise ValueError(
                        f"Table {simple} contains a reserved physical column"
                    )
                physical_schema_versions.setdefault(column_name, []).append(
                    field,
                )
                previous_field = physical_schema_fields.get(column_name)
                if previous_field is None:
                    physical_schema_fields[column_name] = field
                elif not allow_v2_4_timestamp_only:
                    # v2.4 imposed no cross-append dtype contract and stored
                    # only the newest logical schema. Each immutable resource
                    # is decoded and proven independently below, so applying
                    # the stricter current restore lattice here would reject
                    # valid historical numeric, nested, and Enum evolution.
                    if (
                        previous_field.type.equals(field.type)
                        and not previous_field.equals(
                            field.with_nullable(previous_field.nullable),
                            check_metadata=True,
                        )
                        and str(legacy_polars_dtype_for_field(previous_field))
                        != str(legacy_polars_dtype_for_field(field))
                    ):
                        raise ValueError(
                            f"Table {simple} contains incompatible logical "
                            "column metadata"
                        )
                    merged_type = _lossless_restore_physical_type(
                        previous_field.type, field.type,
                    )
                    metadata = (
                        field.metadata
                        if pa.types.is_null(previous_field.type)
                        else previous_field.metadata
                    )
                    physical_schema_fields[column_name] = pa.field(
                        column_name,
                        merged_type,
                        nullable=previous_field.nullable or field.nullable,
                        metadata=metadata,
                    )

            _validate_declared_object_seal(
                resource.get("object_seal"), object_metadata,
            )
            stats_seal = stats_seal_for_metadata(
                resource_path, parquet_metadata,
            )
            for field_name, actual_value in (
                ("footer_sha256", stats_seal.footer_sha256),
                ("stats_rows", stats_seal.stats_rows),
                ("stats_digest", stats_seal.stats_digest),
            ):
                if field_name in resource and resource.get(field_name) != actual_value:
                    raise RuntimeError(
                        f"Table {simple} resource statistics disagree with its footer"
                    )
            canonical_resources.append({
                "file": resource_path,
                "rows": rows,
                "file_size": file_size,
                "columns": columns,
                "object_seal": _object_seal_document(object_metadata),
                "footer_sha256": stats_seal.footer_sha256,
                "stats_rows": stats_seal.stats_rows,
                "stats_digest": stats_seal.stats_digest,
            })
            footer_cache[resource_path] = parquet_metadata

        declared_schema_types = _restored_schema_type_values(
            schema_for_public_validation,
            declared_schema_names,
        )
        if allow_v2_4_timestamp_only:
            # Every v2.4 update used last-write-wins schema metadata. A
            # zero-row update emitted no Parquet resource, and could retain a
            # caller-supplied __rowid__, so neither reserved-column shape can
            # prove that retained files describe the final declared schema.
            from supertable.engine.engine_common import (
                _dtype_ast,
                snapshot_duckdb_type,
            )

            def fixed_array_shape_signature(
                raw_type: str,
            ) -> tuple[tuple[tuple[str, ...], tuple[int, ...]], ...]:
                entries: list[tuple[tuple[str, ...], tuple[int, ...]]] = []

                def visit(node: Any, path: tuple[str, ...]) -> None:
                    kind = node[0]
                    if kind == "array":
                        entries.append((path, tuple(int(v) for v in node[2])))
                        visit(node[1], (*path, "array"))
                    elif kind == "list":
                        visit(node[1], (*path, "list"))
                    elif kind == "struct":
                        for field_name, child in node[1]:
                            visit(child, (*path, "struct", str(field_name)))

                visit(_dtype_ast(raw_type), ())
                return tuple(entries)

            for raw_type in declared_schema_types.values():
                try:
                    # This parser is also the closed persisted-dtype grammar.
                    # Do not require every optional execution engine to map a
                    # historical type: v2.4 validly wrote Time and Null, while
                    # Spark intentionally has no representation for them.
                    snapshot_duckdb_type(raw_type)
                except RuntimeError:
                    raise ValueError(
                        f"Table {simple} schema type is invalid"
                    ) from None
            if canonical_resources:
                if not declared_schema_types and physical_schema_versions:
                    raise ValueError(
                        f"Table {simple} v2.4 declared schema is not "
                        "query-compatible with retained data"
                    )
                import duckdb

                compatibility_connection = duckdb.connect(":memory:")
                try:
                    for name, declared_type in declared_schema_types.items():
                        physical_fields = physical_schema_versions.get(name)
                        if not physical_fields:
                            raise ValueError
                        declared_sql_type = snapshot_duckdb_type(declared_type)
                        declared_array_shapes = fixed_array_shape_signature(
                            declared_type,
                        )
                        if declared_array_shapes:
                            for field in physical_fields:
                                physical_type = str(
                                    legacy_polars_dtype_for_field(field)
                                )
                                if (
                                    fixed_array_shape_signature(physical_type)
                                    != declared_array_shapes
                                ):
                                    raise ValueError
                        physical_sql_types = {
                            snapshot_duckdb_type(
                                str(legacy_polars_dtype_for_field(field)),
                            )
                            for field in physical_fields
                        }
                        type_terms = [
                            *sorted(physical_sql_types),
                            declared_sql_type,
                        ]
                        expression = ",".join(
                            f"CAST(NULL AS {sql_type})"
                            for sql_type in type_terms
                        )
                        resolved_type = compatibility_connection.execute(
                            f"SELECT typeof(coalesce({expression}))"
                        ).fetchone()[0]
                        if (
                            duckdb.sqltype(resolved_type)
                            != duckdb.sqltype(declared_sql_type)
                        ):
                            raise ValueError
                except ValueError:
                    raise ValueError(
                        f"Table {simple} v2.4 declared schema is not "
                        "query-compatible with retained data"
                    ) from None
                except Exception:
                    raise ValueError(
                        f"Table {simple} v2.4 declared schema is not "
                        "query-compatible with retained data"
                    ) from None
                finally:
                    compatibility_connection.close()
            canonical_schema = declared_schema_types
        elif canonical_resources:
            if any(
                name not in physical_schema_fields
                for name in declared_schema_names
            ):
                raise ValueError(
                    f"Table {simple} schema is not present in its resources"
                )
            if physical_schema_fields and not declared_schema_names:
                raise ValueError(f"Table {simple} schema omits physical columns")
            canonical_schema = {
                name: str(legacy_polars_dtype_for_field(
                    physical_schema_fields[name],
                ))
                for name in declared_schema_names
            }
        else:
            canonical_schema = declared_schema_types
            from supertable.engine.engine_common import (
                snapshot_duckdb_type,
                snapshot_spark_type,
            )
            for raw_type in canonical_schema.values():
                try:
                    snapshot_duckdb_type(raw_type)
                    snapshot_spark_type(raw_type)
                except RuntimeError:
                    raise ValueError(
                        f"Table {simple} schema type is invalid"
                    ) from None
        return canonical_resources, footer_cache, canonical_schema


    # ------------------------------------------------------------------ delete
    def delete(
            self,
            role_name: str,
            *,
            authorization_callback: Optional[Callable[[], str]] = None,
            post_delete_cleanup_callback: Optional[Callable[[], Any]] = None,
    ) -> str:
        """Delete this SuperTable's data metadata and storage folder.

        WARNING: This is destructive and intended for admin flows.

        RBAC role/user state is intentionally retained.  It is security
        control data and may only be removed through its dedicated mandatory
        audit boundary; recreating the same SuperTable therefore cannot reset
        or silently widen its prior access policy.

        ``authorization_callback`` refreshes the caller's live role after the
        namespace fence is acquired and again immediately before irreversible
        storage deletion. ``post_delete_cleanup_callback`` is a deliberately
        narrow integration hook: it runs only after authoritative namespace
        deletion has committed and while the renewable namespace lock is still
        held, so a concurrent recreation cannot be mistaken for stale state.
        """
        return self._delete_with_intent(
            role_name=role_name,
            authorization_callback=authorization_callback,
            post_delete_cleanup_callback=post_delete_cleanup_callback,
        )

    def recover_delete(
            self,
            role_name: str,
            *,
            intent_id: str,
            confirm_previous_owner_stopped: bool = False,
            authorization_callback: Optional[Callable[[], str]] = None,
            post_delete_cleanup_callback: Optional[Callable[[], Any]] = None,
    ) -> str:
        """Resume an abandoned namespace deletion after liveness proof."""
        return self._delete_with_intent(
            role_name=role_name,
            recovery_intent_id=intent_id,
            confirm_previous_owner_stopped=confirm_previous_owner_stopped,
            authorization_callback=authorization_callback,
            post_delete_cleanup_callback=post_delete_cleanup_callback,
        )

    def _delete_with_intent(
            self,
            *,
            role_name: str,
            recovery_intent_id: Optional[str] = None,
            confirm_previous_owner_stopped: bool = False,
            authorization_callback: Optional[Callable[[], str]] = None,
            post_delete_cleanup_callback: Optional[Callable[[], Any]] = None,
    ) -> str:
        # Deleting the namespace also deletes every child table.  A scoped
        # ADMIN could otherwise authorize the parent through ``*`` and erase a
        # child that has an exact ``access: deny`` override.  There is no root
        # structural lock that can make a per-child preflight race-free, so the
        # parent destructive operation is reserved for the trusted SUPERADMIN
        # control plane.  SimpleTable.delete remains table-policy aware.
        def _authorize(candidate_role: str) -> str:
            if not isinstance(candidate_role, str) or not candidate_role:
                raise PermissionError(
                    "Current SuperTable deletion authorization is unavailable."
                )
            context = resolve_role_access_context(
                super_name=self.super_name,
                organization=self.organization,
                role_name=candidate_role,
                permission=Permission.CONTROL,
                label="delete this SuperTable",
            )
            if context.role_type is not RoleType.SUPERADMIN:
                raise PermissionError(
                    "Only SUPERADMIN can delete an entire SuperTable namespace."
                )
            return candidate_role

        role_name = _authorize(role_name)

        base_dir = os.path.join(self.organization, self.super_name)
        namespace_token = self.catalog.acquire_namespace_lock(
            self.organization, self.super_name, ttl_s=30, timeout_s=60,
        )
        if not namespace_token:
            raise TimeoutError("Could not acquire the namespace deletion fence")
        leaf_tokens: Dict[str, str] = {}
        stage_tokens: Dict[str, str] = {}
        try:
            if authorization_callback is not None:
                role_name = _authorize(authorization_callback())
            if recovery_intent_id is None:
                clones = self.catalog.find_clones_strict(
                    self.organization,
                    self.super_name,
                    namespace_token=namespace_token,
                )
                if (
                    not isinstance(clones, list)
                    or len(clones) > 10_000
                    or any(not isinstance(clone, str) or not clone for clone in clones)
                ):
                    raise RuntimeError(
                        "Catalog returned an invalid dependent-clone result"
                    )
                if clones:
                    raise PermissionError(
                        "Cannot delete a SuperTable while clones still depend on it"
                    )
            if recovery_intent_id is None:
                intent = self.catalog.begin_namespace_deletion(
                    self.organization,
                    self.super_name,
                    namespace_token=namespace_token,
                )
            else:
                intent = self.catalog.recover_namespace_deletion(
                    self.organization,
                    self.super_name,
                    expected_intent_id=recovery_intent_id,
                    namespace_token=namespace_token,
                    confirm_previous_owner_stopped=(
                        confirm_previous_owner_stopped
                    ),
                )
            intent_id = intent.get("intent_id") if isinstance(intent, dict) else None
            if not intent_id:
                raise RuntimeError("Catalog returned an invalid deletion intent")
            logger.info(
                "[deletion] SuperTable cleanup started; recovery=%s",
                recovery_intent_id is not None,
            )

            # The namespace fence prevents new child initialization and makes
            # snapshot commit Lua fail closed. Drain every already-published
            # child's auto-renewed writer lock before touching storage.
            leaf_marker = "meta:leaf:doc:"
            names = sorted({
                key.rsplit(leaf_marker, 1)[-1]
                for key in self.catalog.scan_leaf_keys(
                    self.organization,
                    self.super_name,
                    resolve_replica=False,
                )
                if leaf_marker in key
            })
            for name in names:
                token = self.catalog.acquire_simple_lock(
                    self.organization,
                    self.super_name,
                    name,
                    ttl_s=30,
                    timeout_s=60,
                )
                if not token:
                    raise TimeoutError("Could not drain a table writer")
                leaf_tokens[name] = token

            # A first-time writer owns a leaf lock before it has any leaf to
            # enumerate. Discover live lock keys as well and require two stable
            # complete scans after the durable parent intent. New entrants now
            # fail their pre-I/O intent check; pre-intent owners are drained.
            stable_leaf_scans = 0
            leaf_scan_rounds = 0
            while stable_leaf_scans < 2:
                leaf_scan_rounds += 1
                if leaf_scan_rounds > 10_000:
                    raise RuntimeError(
                        "Table-writer drain exceeded its stability bound"
                    )
                names = self.catalog.scan_leaf_lock_names(
                    self.organization,
                    self.super_name,
                )
                missing = sorted(set(names).difference(leaf_tokens))
                if not missing:
                    stable_leaf_scans += 1
                    continue
                stable_leaf_scans = 0
                if len(leaf_tokens) + len(missing) > 10_000:
                    raise RuntimeError(
                        "Table-writer drain exceeded its key bound"
                    )
                for name in missing:
                    token = self.catalog.acquire_simple_lock(
                        self.organization,
                        self.super_name,
                        name,
                        ttl_s=30,
                        timeout_s=60,
                    )
                    if not token:
                        raise TimeoutError("Could not drain a table writer")
                    leaf_tokens[name] = token

            # Stage uploads also perform object-store I/O while holding their
            # own renewable lease. Enumerate lock keys rather than only the
            # staging index: a first-time creator owns a lock before it has any
            # metadata to list. Two complete stable scans close discovery churn
            # after the durable namespace intent has made every newly-entering
            # writer fail before storage I/O.
            stable_stage_scans = 0
            stage_scan_rounds = 0
            while stable_stage_scans < 2:
                stage_scan_rounds += 1
                if stage_scan_rounds > 10_000:
                    raise RuntimeError(
                        "Stage-writer drain exceeded its stability bound"
                    )
                names = self.catalog.scan_stage_lock_names(
                    self.organization,
                    self.super_name,
                )
                missing = sorted(set(names).difference(stage_tokens))
                if not missing:
                    stable_stage_scans += 1
                    continue
                stable_stage_scans = 0
                if len(stage_tokens) + len(missing) > 10_000:
                    raise RuntimeError(
                        "Stage-writer drain exceeded its key bound"
                    )
                for name in missing:
                    token = self.catalog.acquire_stage_lock(
                        self.organization,
                        self.super_name,
                        name,
                        ttl_s=30,
                        timeout_s=60,
                    )
                    if not token:
                        raise TimeoutError("Could not drain a staging writer")
                    stage_tokens[name] = token

            # This is the last authorization boundary before irreversible I/O.
            # It runs after every in-flight writer/stager has been drained, so
            # revocation during a long drain cannot authorize the deletion.
            if authorization_callback is not None:
                role_name = _authorize(authorization_callback())

            # Object-store prefixes normally have no exact marker object. The
            # backend operation drains and verifies the full prefix while no
            # existing or newly-created table can publish into it.
            self.storage.delete_prefix(base_dir)

            # Preserve live fence keys throughout the SCAN deletion. Once the
            # catalog root is gone, finally releasing them cannot admit a stale
            # writer: its root/leaf recheck or fenced commit fails.
            self.catalog.delete_super_table(
                self.organization,
                self.super_name,
                namespace_token=namespace_token,
                intent_id=intent_id,
            )
            cleanup_failed = False
            try:
                if post_delete_cleanup_callback is not None:
                    post_delete_cleanup_callback()
                if recovery_intent_id is not None:
                    self.catalog.clear_namespace_deletion_tombstone(
                        self.organization,
                        self.super_name,
                        expected_intent_id=intent_id,
                        namespace_token=namespace_token,
                        confirm_previous_owner_stopped=(
                            confirm_previous_owner_stopped
                        ),
                    )
            except Exception:
                # The storage and authoritative catalog namespace are already
                # gone. Preserve that exact outcome and prohibit callers from
                # retrying either the ordinary or recovery deletion.
                cleanup_failed = True
            if cleanup_failed:
                raise NamespaceCleanupPostCommitError(str(intent_id)) from None
        finally:
            for name, token in reversed(list(stage_tokens.items())):
                self.catalog.release_stage_lock(
                    self.organization, self.super_name, name, token,
                )
            for name, token in reversed(list(leaf_tokens.items())):
                self.catalog.release_simple_lock(
                    self.organization, self.super_name, name, token,
                )
            self.catalog.release_namespace_lock(
                self.organization, self.super_name, namespace_token,
            )
        return str(intent_id)

    @classmethod
    def recover_pending_delete(
            cls,
            *,
            organization: str,
            super_name: str,
            role_name: str,
            intent_id: str,
            confirm_previous_owner_stopped: bool = False,
            authorization_callback: Optional[Callable[[], str]] = None,
            post_delete_cleanup_callback: Optional[Callable[[], Any]] = None,
    ) -> str:
        """Recover a namespace after its root was already removed."""
        table = cls.__new__(cls)
        table.identity = "super"
        table.super_name = super_name
        table.organization = organization
        table.storage = get_storage()
        table.catalog = RedisCatalog()
        table.super_dir = os.path.join(organization, super_name, "super")
        return table.recover_delete(
            role_name,
            intent_id=intent_id,
            confirm_previous_owner_stopped=confirm_previous_owner_stopped,
            authorization_callback=authorization_callback,
            post_delete_cleanup_callback=post_delete_cleanup_callback,
        )
