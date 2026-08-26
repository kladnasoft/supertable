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


_LEGACY_METADATA_MIGRATION_VERSION = 1
_MAX_MIGRATION_SNAPSHOT_BYTES = 8 * 1024 * 1024
_MAX_MIGRATION_TABLES = 10_000
_MAX_MIGRATION_LEAF_INDEX_BYTES = 16 * 1024 * 1024
_MAX_MIGRATION_SCAN_CALLS = 1_000
_MAX_MIGRATION_COLUMN_CHUNKS = 100_000


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

    def migrate_legacy_metadata(self) -> Dict[str, Any]:
        """Verify and upgrade legacy table snapshots in this supertable.

        The migration is deliberately explicit: every referenced data and
        tombstone object must be readable before a successor snapshot is
        published.  Missing statistics are rebuilt from Parquet footers; no
        rows or deletion-vector entries are invented.
        """
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
                    )
                    if was_migrated:
                        migrated.append(simple)
                    else:
                        # A current snapshot performs no commit, so its long
                        # resource/tombstone validation needs an explicit
                        # fence at the equivalent completion boundary.
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
                                f"Table {simple} changed during migration validation"
                            )
                finally:
                    self.catalog.release_simple_lock(
                        self.organization,
                        self.super_name,
                        simple,
                        simple_token,
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

        cached_snapshot = complete_snapshot_payload(
            leaf.get("payload"),
            expected_version=version,
            require_policy_marker=True,
        )
        if cached_snapshot is not None:
            def redis_lua_comparison_value(value: Any) -> Any:
                # The commit scripts decode and re-encode the payload with
                # Redis Lua cjson.  Model its two known lossy lanes on both
                # sides: an empty object becomes an empty array, and integers
                # outside the IEEE-754 exact range collapse to their double
                # representation.  Differences outside the same unavoidable
                # equivalence bucket remain visible.
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
                if (
                    type(value) is int
                    and abs(value) > (1 << 53) - 1
                ):
                    return float(value)
                return value

            def cache_comparison_document(document: Dict[str, Any]) -> Any:
                return redis_lua_comparison_value(
                    snapshot_cache_payload(document),
                )

            if cache_comparison_document(
                cached_snapshot,
            ) != cache_comparison_document(snapshot):
                raise RuntimeError(
                    f"Table {simple} Redis snapshot cache disagrees with storage"
                )
        preserve_current_rowids = False
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
            from supertable.row_identity import snapshot_proves_stable_rowids
            preserve_current_rowids = snapshot_proves_stable_rowids(snapshot)
        successor = copy.deepcopy(snapshot)
        successor["simple_name"] = simple
        successor["location"] = table_dir
        resources, footer_cache, canonical_schema = (
            self._validate_legacy_resources(simple=simple, snapshot=successor)
        )
        successor["resources"] = resources
        successor["schema"] = canonical_schema
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
            from supertable.row_identity import snapshot_proves_stable_rowids
            if not snapshot_proves_stable_rowids(successor):
                raise RuntimeError(
                    f"Table {simple} stable row-ID proof could not be preserved"
                )
        else:
            successor.pop("rowid_high_watermark", None)

        self._migrate_legacy_tombstone(
            simple=simple,
            snapshot=successor,
            version=version,
            allowed_files=set(data_paths),
            available_rows=sum(resource["rows"] for resource in resources),
        )

        stats_rows = extract_stats_rows(
            data_paths,
            footer_md_cache=footer_cache,
            storage=self.storage,
            max_rows=MAX_SHOW_STATS_ROWS,
            max_decoded_bytes=MAX_SHOW_STATS_DECODED_BYTES,
        )
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
            validated_stats = load_bounded_stats_diagnostic(
                generated_stats_path,
                expected_rows=stats_row_count,
                storage=self.storage,
                max_rows=MAX_SHOW_STATS_ROWS,
                max_decoded_bytes=MAX_SHOW_STATS_DECODED_BYTES,
            )
            expected_stats_seals = {
                resource["file"]: seal
                for resource in resources
                for seal in [resource_stats_seal(resource)]
                if seal is not None and seal.stats_rows > 0
            }
            if stats_resource_seals(validated_stats) != expected_stats_seals:
                raise RuntimeError(
                    f"Table {simple} generated statistics failed seal validation"
                )
        successor["stats_file"] = generated_stats_path
        successor["stats_rows"] = stats_row_count
        successor["_legacy_metadata_migration_version"] = (
            _LEGACY_METADATA_MIGRATION_VERSION
        )
        successor["snapshot_version"] = version + 1
        successor["previous_snapshot"] = path
        now_ms = int(time.time() * 1000)
        successor["last_updated_ms"] = now_ms
        new_path = f"{self.organization}/{self.super_name}/tables/{simple}/snapshots/{generate_filename(alias=simple)}"
        commit_id = uuid.uuid4().hex
        self.storage.write_json(new_path, successor)
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
            ):
                raise
        return True

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
            if type(migration_version) is not int or migration_version < 1:
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
        }
        # A migration successor deliberately cannot claim stable-row-ID
        # authority, and older snapshots may not have a row filter.  Its
        # marker may waive only those two fields, not the identity, counters,
        # schema, resource, tombstone, or statistics shape written by the
        # migration itself.
        if migration_version is None:
            required_snapshot_fields.update({
                "rowid_high_watermark",
                "_row_filter",
            })
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
            return False
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
    ) -> None:
        """Validate a v1/v2/v3 deletion vector and bind it to the successor."""
        import polars as pl

        from supertable.processing import (
            load_tombstone_manifest_from_storage,
            persist_tombstone_manifest_v2,
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
            TOMBSTONE_FORMAT_V1,
            TOMBSTONE_FORMAT_V2,
            normalize_snapshot_tombstone_state,
        )

        state = normalize_snapshot_tombstone_state(snapshot)
        if state.pointer is None:
            snapshot["tombstone"] = None
            snapshot["tombstone_rows"] = 0
            snapshot["tombstone_digest"] = None
            if state.format_present:
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
        if not publish_successor:
            return
        if state.tombstone_format == TOMBSTONE_FORMAT_V2:
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

        declared_schema_names = _restored_schema_field_names(schema)
        data_dir = os.path.join(
            self.organization, self.super_name, "tables", simple, "data",
        )
        seen_resources: set[str] = set()
        physical_schema_folded: dict[str, str] = {}
        physical_schema_types: dict[str, pa.DataType] = {}
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
            columns = int(parquet_metadata.num_columns)
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
                max(1, int(parquet_metadata.num_row_groups)) * columns
            )
            if total_rows > 1_000_000_000 or total_bytes > 2 * 1024 ** 4:
                raise ValueError(f"Table {simple} aggregate resource bounds are invalid")
            if total_footer_bytes > 256 * 1024 * 1024:
                raise ValueError(f"Table {simple} footer bytes exceed the safety limit")
            if total_column_chunks > _MAX_MIGRATION_COLUMN_CHUNKS:
                raise ValueError(f"Table {simple} footer fan-out exceeds the safety limit")

            arrow_schema = parquet_metadata.schema.to_arrow_schema()
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
                if folded_name in {
                    "__rowid__", "__timestamp__", "__file__",
                    "__supertable_source_file__",
                    "__supertable_scan_filename__",
                } or folded_name.startswith("__supertable_"):
                    continue
                previous_dtype = physical_schema_types.get(column_name)
                physical_schema_types[column_name] = (
                    field.type
                    if previous_dtype is None
                    else _lossless_restore_physical_type(
                        previous_dtype, field.type,
                    )
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

        if canonical_resources:
            if any(
                name not in physical_schema_types
                for name in declared_schema_names
            ):
                raise ValueError(
                    f"Table {simple} schema is not present in its resources"
                )
            if physical_schema_types and not declared_schema_names:
                raise ValueError(f"Table {simple} schema omits physical columns")
            canonical_schema = {
                name: str(_polars_dtype_for_arrow_field(pa.field(
                    name, physical_schema_types[name],
                )))
                for name in declared_schema_names
            }
        else:
            canonical_schema = _restored_schema_type_values(
                schema, declared_schema_names,
            )
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
