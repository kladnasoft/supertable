# supertable/staging_area.py
from __future__ import annotations

import os
import json
import re
import tempfile
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

import pyarrow as pa

from supertable.config.defaults import logger
from supertable import redis_keys as RK
from supertable.storage.storage_factory import get_storage
from supertable.redis_catalog import DeletionIntentConflictError, RedisCatalog
from supertable.rbac.access_control import (
    check_control_access,
    check_create_access,
    check_meta_access,
    check_read_access,
    check_write_access,
)

_SAFE_STAGE_RE = re.compile(
    r"^(__[a-z0-9][a-z0-9_-]{0,59}__|[a-z0-9][a-z0-9_-]{0,63})$"
)
_SAFE_FILE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,511}$")
_MAX_STAGE_FILES = 10_000
_MAX_STAGE_META_BYTES = 8 * 1024 * 1024
_MAX_STAGING_FILE_BYTES = 512 * 1024 * 1024


def _safe_path_component(value: str, *, label: str) -> str:
    """Return one storage path component or reject traversal/absolute forms."""
    component = str(value or "")
    if (
        not component
        or component in (".", "..")
        or os.path.isabs(component)
        or "/" in component
        or "\\" in component
        or "\x00" in component
        or component != component.strip()
        or _SAFE_FILE_RE.fullmatch(component) is None
    ):
        raise ValueError(f"Invalid {label}: expected one non-empty path component")
    return component


def _safe_stage_name(value: str) -> str:
    component = _safe_path_component(value, label="staging_name")
    if _SAFE_STAGE_RE.fullmatch(component) is None:
        raise ValueError("Invalid staging_name")
    return component


def _join_contained(base: str, *components: str) -> str:
    """Join components and prove the resulting path remains below ``base``."""
    candidate = os.path.normpath(os.path.join(base, *components))
    base_abs = os.path.abspath(os.path.normpath(base))
    candidate_abs = os.path.abspath(candidate)
    if os.path.commonpath((base_abs, candidate_abs)) != base_abs:
        raise ValueError("Staging path escaped its configured namespace")
    return candidate


def _resolve_super_name(super_table: Any) -> Optional[str]:
    """
    Best-effort resolution for backwards compatibility.

    Some examples pass:
        Staging(super_table=SuperTable(...), organization=...)

    We try common attribute names.
    """
    if super_table is None:
        return None
    for attr in ("super_name", "name", "supertable", "table_name"):
        try:
            v = getattr(super_table, attr)
        except Exception:
            continue
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


class Staging:
    """
    Staging implementation (supports TWO modes):

    1) Manager mode (backward-compatible with examples/3.4. read_staging.py):
        staging = Staging(super_table=super_table, organization=organization)
        staging.get_directory_structure()

       In this mode, you can inspect staging structure and open a specific stage.

    2) Stage mode (current implementation):
        stage = Staging(organization=org, super_name=sup, staging_name="my_stage")
        stage.save_as_parquet(...)
        stage.list_files()
        stage.delete()

    Notes
    -----
    - Physical layout:
        {org}/{super}/staging/{stage_name}/<files...>
        {org}/{super}/staging/{stage_name}_files.json   (flat index)

    - Redis meta:
        supertable:{org}:lakes:{sup}:meta:staging:doc:{stage_name}:meta
        plus a staging:index SET for fast listing.
    """

    def __init__(
        self,
        *,
        organization: str,
        super_name: Optional[str] = None,
        super_table: Any = None,
        staging_name: Optional[str] = None,
    ):
        self.organization = organization

        # Back-compat: accept super_table instead of super_name
        resolved = super_name or _resolve_super_name(super_table)
        if not resolved:
            raise TypeError(
                "Staging requires either super_name='...' or super_table=<SuperTable ...> "
                "(with attribute .super_name or .name)."
            )
        self.super_name = resolved

        self.storage = get_storage()
        self.catalog = RedisCatalog()

        self._check_deletion_intent_absent()

        # Validate supertable existence
        if not self.catalog.root_exists(self.organization, self.super_name):
            raise FileNotFoundError(f"SuperTable does not exist: {self.organization}/{self.super_name}")

        self.base_staging_dir = os.path.join(self.organization, self.super_name, "staging")

        # Mode
        self.staging_name = staging_name
        self._is_manager = staging_name is None

        if not self._is_manager:
            # Stage mode paths
            assert self.staging_name is not None
            self.staging_name = _safe_stage_name(self.staging_name)
            self._check_deletion_intent_absent(stage=self.staging_name)
            self.stage_dir = _join_contained(
                self.base_staging_dir, self.staging_name,
            )
            self.files_index_path = _join_contained(
                self.base_staging_dir, f"{self.staging_name}_files.json",
            )
            # Construction/open is read-only.  Stage resources are created
            # lazily inside the first authorized write operation; otherwise a
            # caller could mutate storage and Redis without ever presenting a
            # role.
        else:
            # Manager mode doesn't create anything; just sets placeholders.
            self.stage_dir = None
            self.files_index_path = None

    def _check_deletion_intent_absent(
            self, *, stage: Optional[str] = None,
    ) -> None:
        guard = getattr(
            type(self.catalog), "check_deletion_intent_absent", None,
        )
        if callable(guard):
            self.catalog.check_deletion_intent_absent(
                self.organization,
                self.super_name,
                stage=stage,
            )

    # --------------------------------------------------------------------- #
    # Manager-mode helpers
    # --------------------------------------------------------------------- #

    def open(self, staging_name: str) -> "Staging":
        """Open a specific stage (returns a stage-mode instance)."""
        return Staging(
            organization=self.organization,
            super_name=self.super_name,
            staging_name=staging_name,
        )

    def get_directory_structure(
        self, role_name: str, *, max_stages: int = 10_000,
    ) -> Dict[str, Any]:
        """
        Backwards-compatible method used by examples/3.4. read_staging.py.

        Returns a JSON-serializable dict describing the staging area and stages.
        """
        check_meta_access(
            super_name=self.super_name,
            organization=self.organization,
            role_name=role_name,
            table_name=self.super_name,
        )
        self._check_deletion_intent_absent()
        # Ensure base exists (read-only; no lock)
        base_exists = self.storage.exists(self.base_staging_dir)
        if type(max_stages) is not int or max_stages <= 0:
            raise ValueError("max_stages must be a positive integer")
        stagings = self.catalog.list_stagings(
            self.organization, self.super_name, limit=max_stages,
        )

        stages: List[Dict[str, Any]] = []
        for name in sorted(stagings):
            try:
                safe_name = _safe_stage_name(name)
                self._check_deletion_intent_absent(stage=safe_name)
            except DeletionIntentConflictError:
                # A deleting/deleted stage is deliberately non-live even if
                # stale metadata or a fixed-path index is still present.
                continue
            stage_dir = _join_contained(self.base_staging_dir, safe_name)
            files_index_path = _join_contained(
                self.base_staging_dir, f"{safe_name}_files.json",
            )

            stage_meta, redis_files = self._get_authoritative_stage_files(
                stage_name=safe_name,
                files_index_path=files_index_path,
            )
            files = sorted({
                str(item.get("file"))
                for item in redis_files.values()
                if isinstance(item, dict) and item.get("file")
            })

            stages.append(
                {
                    "name": safe_name,
                    "path": stage_dir,
                    "exists": bool(self.storage.exists(stage_dir)),
                    "files_index_path": files_index_path,
                    "files_index_exists": bool(self.storage.exists(files_index_path)),
                    "file_count": len(files),
                    "files": files,
                    "redis_meta": stage_meta or {},
                }
            )

        return {
            "organization": self.organization,
            "super_name": self.super_name,
            "base_staging_dir": self.base_staging_dir,
            "base_exists": bool(base_exists),
            "stages": stages,
            "stage_count": len(stages),
        }

    # --------------------------------------------------------------------- #
    # Stage-mode operations
    # --------------------------------------------------------------------- #

    def _require_stage_mode(self) -> str:
        if self._is_manager or self.staging_name is None:
            raise RuntimeError(
                "This Staging instance is in manager mode (no staging_name was provided). "
                "Call .open(staging_name) first."
            )
        return self.staging_name

    def _with_stage_lock(self, stage_name: str, fn):
        stage_name = _safe_stage_name(stage_name)
        # RedisLocking renews this lease at half-TTL until the ownership-checked
        # release. Cloud prefix deletion can legitimately take longer than one
        # lease period; a fixed SET NX EX lock would allow a concurrent save to
        # enter mid-delete and then be orphaned when metadata is removed.
        token = self.catalog.acquire_stage_lock(
            self.organization,
            self.super_name,
            stage_name,
            ttl_s=30,
            timeout_s=30,
        )
        if not token:
            raise RuntimeError(f"Stage {stage_name} is currently locked by another process.")

        try:
            return fn(token)
        finally:
            self.catalog.release_stage_lock(
                self.organization,
                self.super_name,
                stage_name,
                token,
            )

    def _with_lock(self, fn):
        return self._with_stage_lock(self._require_stage_mode(), fn)

    @staticmethod
    def _fresh_role(
        role_name: str,
        authorization_callback: Optional[Callable[[], str]],
    ) -> str:
        effective = (
            authorization_callback()
            if authorization_callback is not None else role_name
        )
        if not isinstance(effective, str) or not effective.strip():
            raise PermissionError("A current authorized role is required")
        return effective.strip()

    def _read_legacy_file_map(
            self,
            *,
            files_index_path: str,
    ) -> Dict[str, Dict[str, Any]]:
        """Load only safe legacy index entries before one-time migration."""
        if not self.storage.exists(files_index_path):
            return {}
        raw_index: Any = self.storage.read_json(files_index_path) or []
        if not isinstance(raw_index, list):
            raise RuntimeError("Legacy staging file index must be a JSON list")

        files: Dict[str, Dict[str, Any]] = {}
        for item in raw_index:
            if not isinstance(item, dict):
                continue
            raw_name = item.get("file")
            if not isinstance(raw_name, str):
                continue
            try:
                file_name = _safe_path_component(
                    raw_name, label="legacy staging file name",
                )
            except ValueError as exc:
                logger.error(
                    "[staging] ignoring unsafe legacy file entry %r: %s",
                    raw_name, exc,
                )
                continue
            normalized = dict(item)
            normalized["file"] = file_name
            # Legacy writers append, so the last duplicate is the state readers
            # historically observed.  Redis turns that list into a keyed map.
            files[file_name] = normalized
        return files

    def _get_authoritative_stage_files(
            self,
            *,
            stage_name: str,
            files_index_path: str,
            lock_token: Optional[str] = None,
            known_meta: Optional[Dict[str, Any]] = None,
    ) -> tuple[Optional[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """Return the Redis file map, migrating a legacy index exactly once.

        A Redis ``files`` dictionary, including ``{}``, is authoritative.  A
        fixed-path JSON index is consulted only for an older stage document
        where the field is absent, and is then published while the stage lease
        and both deletion intents are checked by the catalog Lua boundary.
        """
        stage_name = _safe_stage_name(stage_name)

        def _resolve(token: str):
            stage_meta = self.catalog.get_staging_meta(
                self.organization, self.super_name, stage_name,
            )
            if stage_meta is None:
                return None, {}
            if "files" in stage_meta:
                file_map = stage_meta.get("files")
                if not isinstance(file_map, dict):
                    raise RuntimeError(
                        f"Corrupt staging file metadata for "
                        f"{self.organization}/{self.super_name}/{stage_name}"
                    )
                return stage_meta, file_map

            migrated = self._read_legacy_file_map(
                files_index_path=files_index_path,
            )
            next_meta = dict(stage_meta)
            next_meta["files"] = migrated
            self.catalog.upsert_staging_meta(
                self.organization,
                self.super_name,
                stage_name,
                meta=next_meta,
                lock_token=token,
            )
            persisted = self.catalog.get_staging_meta(
                self.organization, self.super_name, stage_name,
            )
            if not isinstance(persisted, dict) or not isinstance(
                    persisted.get("files"), dict,
            ):
                raise RuntimeError(
                    f"Staging file-index migration was not durable for "
                    f"{self.organization}/{self.super_name}/{stage_name}"
                )
            return persisted, persisted["files"]

        initial = known_meta
        if initial is None:
            initial = self.catalog.get_staging_meta(
                self.organization, self.super_name, stage_name,
            )
        if initial is None:
            return None, {}
        if "files" in initial:
            initial_files = initial.get("files")
            if not isinstance(initial_files, dict):
                raise RuntimeError(
                    f"Corrupt staging file metadata for "
                    f"{self.organization}/{self.super_name}/{stage_name}"
                )
            return initial, initial_files
        if lock_token is not None:
            return _resolve(lock_token)
        # META reads remain observational.  Until the next authorized writer
        # performs the fenced one-time migration, preserve legacy visibility
        # transiently without acquiring a lease or changing Redis.
        return initial, self._read_legacy_file_map(
            files_index_path=files_index_path,
        )

    def _init_stage(
            self, lock_token: str, *, existing_meta: Optional[dict],
    ) -> Optional[Dict[str, Any]]:
        """Prepare storage and return an unpublished first-stage document.

        Initial Redis publication is deliberately deferred until the first
        parquet object and the legacy compatibility index have both been
        written successfully.  A failed first upload therefore cannot consume
        the stage name or change the next authorization decision from CREATE
        to WRITE.
        """
        stage_name = self._require_stage_mode()

        # A stage lock can expire while its former owner is paused inside an
        # object-store call.  The no-TTL intent, not lease absence, decides
        # whether this fixed prefix may be recreated.
        self.catalog.check_stage_mutation_allowed(
            self.organization,
            self.super_name,
            stage_name,
            lock_token=lock_token,
        )

        # 1) Create the physical folder (staging/{staging_name})
        if not self.storage.exists(self.stage_dir):
            self.storage.makedirs(self.stage_dir)

        # 2) Keep the legacy storage index readable for upgraded deployments.
        # New entries are published only through the Redis-fenced stage document;
        # a fixed storage JSON object cannot be protected from an expired writer.
        if not self.storage.exists(self.files_index_path):
            empty_legacy_index: Any = []
            self.storage.write_json(self.files_index_path, empty_legacy_index)

        if existing_meta is not None:
            return None
        return {
            "path": self.stage_dir,
            "created_at_ms": int(time.time() * 1000),
            "files": {},
        }

    def create(
        self,
        role_name: str,
        *,
        authorization_callback: Optional[Callable[[], str]] = None,
    ) -> str:
        """Create an empty authoritative stage under the renewable stage lock."""
        stage_name = self._require_stage_mode()

        def _op(lock_token: str) -> str:
            effective_role = self._fresh_role(
                role_name, authorization_callback,
            )
            self.catalog.check_stage_mutation_allowed(
                self.organization,
                self.super_name,
                stage_name,
                lock_token=lock_token,
            )
            existing = self.catalog.get_staging_meta(
                self.organization, self.super_name, stage_name,
            )
            if existing is not None:
                check_write_access(
                    super_name=self.super_name,
                    organization=self.organization,
                    role_name=effective_role,
                    table_name=self.super_name,
                )
                return stage_name
            check_create_access(
                super_name=self.super_name,
                organization=self.organization,
                role_name=effective_role,
                table_name=self.super_name,
            )
            initial = self._init_stage(lock_token, existing_meta=None)
            if not isinstance(initial, dict):
                raise RuntimeError("Missing initial staging metadata")
            self.catalog.upsert_staging_meta(
                self.organization,
                self.super_name,
                stage_name,
                meta=initial,
                lock_token=lock_token,
                create_only=True,
            )
            return stage_name

        return self._with_lock(_op)

    def save_as_parquet(self, *, role_name: str, arrow_table: pa.Table, base_file_name: str,
                        source: str = "upload", duration_ms: float = 0, pipe_name: str = "", pipe_id: str = "",
                        authorization_callback: Optional[Callable[[], str]] = None) -> str:
        stage_name = self._require_stage_mode()
        if not isinstance(arrow_table, pa.Table):
            raise TypeError("Staging input must be a PyArrow table")
        try:
            schema_bytes = int(arrow_table.schema.serialize().size)
        except Exception:
            schema_bytes = len(str(arrow_table.schema).encode("utf-8"))
        if (
            arrow_table.num_rows > 5_000_000
            or arrow_table.num_columns > 4096
            or int(arrow_table.nbytes) > 512 * 1024 * 1024
            or schema_bytes > 1024 * 1024
        ):
            raise ValueError("Staging input exceeds its row/schema/memory limit")
        original_name = os.path.basename(str(base_file_name or ""))
        text_fields = {
            "original file name": original_name,
            "source": source,
            "pipe_name": pipe_name,
            "pipe_id": pipe_id,
        }
        limits = {
            "original file name": 1024,
            "source": 128,
            "pipe_name": 256,
            "pipe_id": 256,
        }
        for label, value in text_fields.items():
            if (
                not isinstance(value, str)
                or "\x00" in value
                or "\r" in value
                or "\n" in value
                or len(value.encode("utf-8")) > limits[label]
            ):
                raise ValueError(f"Invalid staging {label}")
        def _op(_lock_token: str):
            effective_role = self._fresh_role(
                role_name, authorization_callback,
            )
            existing_meta = self.catalog.get_staging_meta(
                self.organization, self.super_name, stage_name,
            )
            access = check_write_access if existing_meta is not None else check_create_access
            access(
                super_name=self.super_name,
                organization=self.organization,
                role_name=effective_role,
                table_name=self.super_name,
            )
            existing_files: Dict[str, Dict[str, Any]] = {}
            if existing_meta is not None:
                existing_meta, existing_files = self._get_authoritative_stage_files(
                    stage_name=stage_name,
                    files_index_path=self.files_index_path,
                    lock_token=_lock_token,
                    known_meta=existing_meta,
                )
                if len(existing_files) >= _MAX_STAGE_FILES:
                    raise ValueError("Staging file fan-out exceeds its safety limit")
            initial_meta = self._init_stage(
                _lock_token, existing_meta=existing_meta,
            )
            ts_ns = time.time_ns()
            # The caller-supplied display name is metadata only.  Never let it
            # select a filesystem/object key; a server-generated component also
            # prevents collisions between concurrent/retried uploads.
            file_name = f"stage_{ts_ns}_{uuid.uuid4().hex}.parquet"
            file_path = _join_contained(self.stage_dir, file_name)

            try:
                # Revalidate immediately before irreversible object I/O. The
                # first callback may have run before a slow legacy migration.
                effective_role = self._fresh_role(
                    effective_role, authorization_callback,
                )
                access(
                    super_name=self.super_name,
                    organization=self.organization,
                    role_name=effective_role,
                    table_name=self.super_name,
                )
                self.storage.write_parquet(arrow_table, file_path)
                file_size = int(self.storage.size(file_path))
                object_identity = self.storage.stat_object(
                    file_path,
                ).identity_token()
                if (
                    file_size <= 0
                    or file_size > _MAX_STAGING_FILE_BYTES
                    or not isinstance(object_identity, str)
                    or not object_identity
                    or len(object_identity.encode("utf-8")) > 4096
                ):
                    raise RuntimeError(
                        "Staging storage cannot prove the uploaded object identity"
                    )
            except Exception:
                try:
                    self.storage.delete(file_path)
                except Exception:
                    logger.warning(
                        "[staging] failed to clean an unindexed upload object"
                    )
                raise

            file_meta = {
                "file": file_name, "written_at_ns": ts_ns,
                "rows": arrow_table.num_rows, "source": source,
                "file_size": max(0, file_size),
                "memory_bytes": max(0, int(arrow_table.nbytes)),
                "column_count": max(0, int(arrow_table.num_columns)),
                "schema_bytes": max(0, schema_bytes),
                "object_identity": object_identity,
                "duration_ms": round(duration_ms) if duration_ms else None,
                "pipe_name": pipe_name or None, "pipe_id": pipe_id or None,
                "status": "ok",
                "original_name": original_name or None,
            }
            try:
                encoded_file_meta = json.dumps(
                    file_meta,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8")
            except (TypeError, ValueError, OverflowError) as exc:
                try:
                    self.storage.delete(file_path)
                except Exception:
                    pass
                raise ValueError("Staging file metadata is invalid") from exc
            if len(encoded_file_meta) > 64 * 1024:
                try:
                    self.storage.delete(file_path)
                except Exception:
                    pass
                raise ValueError("Staging file metadata exceeds its size limit")

            # A role may be revoked while the object store is writing. Refuse
            # publication before the Redis boundary; the unique object is then
            # safely orphaned for lifecycle cleanup.
            try:
                effective_role = self._fresh_role(
                    effective_role, authorization_callback,
                )
                access(
                    super_name=self.super_name,
                    organization=self.organization,
                    role_name=effective_role,
                    table_name=self.super_name,
                )
            except Exception:
                try:
                    self.storage.delete(file_path)
                except Exception:
                    logger.warning(
                        "[staging] failed to clean a revoked unindexed upload"
                    )
                raise
            # The lock, deletion intents, and metadata update are checked in one
            # Redis Lua boundary. A stale process can leave at most its unique
            # parquet object orphaned; it cannot overwrite a newer live index.
            if existing_meta is None:
                # First publication is a single create-only Redis transaction
                # containing the first file. If this lease expired while the
                # object store was blocked, a newer owner wins and its stage
                # document is never overwritten.
                if initial_meta is None:
                    raise RuntimeError("Missing initial staging metadata")
                first_meta = dict(initial_meta)
                first_meta["files"] = {file_name: file_meta}
                self.catalog.upsert_staging_meta(
                    self.organization,
                    self.super_name,
                    stage_name,
                    meta=first_meta,
                    lock_token=_lock_token,
                    create_only=True,
                )
            else:
                self.catalog.upsert_staging_file_meta(
                    self.organization,
                    self.super_name,
                    stage_name,
                    file_name,
                    meta=file_meta,
                    lock_token=_lock_token,
                )

            logger.info(f"[staging] saved {file_name} and updated its fenced index")
            return file_name

        return self._with_lock(_op)

    def list_files(self, role_name: str) -> List[str]:
        stage_name = self._require_stage_mode()
        check_meta_access(
            super_name=self.super_name,
            organization=self.organization,
            role_name=role_name,
            table_name=self.super_name,
        )
        self._check_deletion_intent_absent(stage=stage_name)
        _stage_meta, redis_files = self._get_authoritative_stage_files(
            stage_name=stage_name,
            files_index_path=self.files_index_path,
        )
        return sorted({
            str(item.get("file"))
            for item in redis_files.values()
            if isinstance(item, dict) and item.get("file")
        })

    def list_file_metadata(self, role_name: str) -> List[Dict[str, Any]]:
        """Return the bounded, Redis-authoritative stage file documents."""
        stage_name = self._require_stage_mode()
        check_meta_access(
            super_name=self.super_name,
            organization=self.organization,
            role_name=role_name,
            table_name=self.super_name,
        )
        self._check_deletion_intent_absent(stage=stage_name)
        _meta, files = self._get_authoritative_stage_files(
            stage_name=stage_name,
            files_index_path=self.files_index_path,
        )
        if len(files) > 10_000:
            raise RuntimeError("Staging file index exceeds its safety limit")
        result: List[Dict[str, Any]] = []
        for key, raw in files.items():
            if not isinstance(key, str) or not isinstance(raw, dict):
                raise RuntimeError("Corrupt staging file metadata")
            safe_name = _safe_path_component(key, label="staging file name")
            if raw.get("file") != safe_name:
                raise RuntimeError("Corrupt staging file metadata")
            projected = dict(raw)
            projected.pop("object_identity", None)
            result.append(projected)
        return sorted(
            result,
            key=lambda item: int(item.get("written_at_ns") or 0),
        )

    def read_parquet_files(
        self,
        role_name: str,
        *,
        file_names: Optional[List[str]] = None,
        max_files: int = 256,
        max_rows: int = 5_000_000,
        max_bytes: int = 512 * 1024 * 1024,
        max_columns: int = 4096,
        max_schema_bytes: int = 1024 * 1024,
        require_bounded_metadata: bool = False,
        authorization_callback: Optional[Callable[[], str]] = None,
    ) -> List[pa.Table]:
        """Read a pinned stage selection under one renewable stage lease.

        File names must be present in the Redis-authoritative map.  Every
        cumulative fan-out, row, memory and schema bound is checked before the
        caller can concatenate the tables.
        """
        stage_name = self._require_stage_mode()
        check_read_access(
            super_name=self.super_name,
            organization=self.organization,
            role_name=role_name,
            table_name=self.super_name,
            require_unfiltered=True,
        )
        limits = (max_files, max_rows, max_bytes, max_columns, max_schema_bytes)
        if any(type(value) is not int or value <= 0 for value in limits):
            raise ValueError("Staging read limits must be positive integers")
        requested = None
        if file_names is not None:
            if not isinstance(file_names, list) or not file_names:
                raise ValueError("file_names must be a non-empty list")
            requested = [
                _safe_path_component(name, label="staging file name")
                for name in file_names
            ]
            if len(set(requested)) != len(requested):
                raise ValueError("Duplicate staging file name")

        def _op(lock_token: str) -> List[pa.Table]:
            effective_role = self._fresh_role(
                role_name, authorization_callback,
            )
            check_read_access(
                super_name=self.super_name,
                organization=self.organization,
                role_name=effective_role,
                table_name=self.super_name,
                require_unfiltered=True,
            )
            self.catalog.check_stage_mutation_allowed(
                self.organization,
                self.super_name,
                stage_name,
                lock_token=lock_token,
            )
            meta, files = self._get_authoritative_stage_files(
                stage_name=stage_name,
                files_index_path=self.files_index_path,
                lock_token=lock_token,
            )
            if meta is None:
                raise FileNotFoundError(f"Staging '{stage_name}' does not exist")
            selected = requested or sorted(
                files,
                key=lambda name: int(
                    (files.get(name) or {}).get("written_at_ns") or 0
                ),
            )
            if not selected:
                return []
            if len(selected) > max_files:
                raise ValueError("Staging file fan-out exceeds its safety limit")

            tables: List[pa.Table] = []
            total_rows = 0
            total_bytes = 0
            for file_name in selected:
                raw_meta = files.get(file_name)
                if not isinstance(raw_meta, dict) or raw_meta.get("file") != file_name:
                    raise FileNotFoundError("Staging file is unavailable")
                bounded_values = {
                    "rows": raw_meta.get("rows"),
                    "file_size": raw_meta.get("file_size"),
                    "memory_bytes": raw_meta.get("memory_bytes"),
                    "column_count": raw_meta.get("column_count"),
                    "schema_bytes": raw_meta.get("schema_bytes"),
                }
                if require_bounded_metadata and any(
                    not isinstance(value, int)
                    or isinstance(value, bool)
                    or value < 0
                    for value in bounded_values.values()
                ):
                    raise RuntimeError(
                        "Legacy staging metadata lacks a safe read bound"
                    )
                declared_rows = bounded_values["rows"]
                declared_file_size = bounded_values["file_size"]
                declared_memory = bounded_values["memory_bytes"]
                declared_columns = bounded_values["column_count"]
                declared_schema = bounded_values["schema_bytes"]
                declared_identity = raw_meta.get("object_identity")
                if require_bounded_metadata and (
                    not isinstance(declared_identity, str)
                    or not declared_identity
                    or len(declared_identity.encode("utf-8")) > 4096
                ):
                    raise RuntimeError(
                        "Staging metadata lacks a stable object identity"
                    )
                if require_bounded_metadata and (
                    total_rows + declared_rows > max_rows
                    or total_bytes + max(declared_file_size, declared_memory) > max_bytes
                    or declared_columns > max_columns
                    or declared_schema > max_schema_bytes
                ):
                    raise ValueError("Staging read exceeds its declared safety limits")
                path = _join_contained(self.stage_dir, file_name)
                try:
                    stored_bytes = int(self.storage.size(path))
                except FileNotFoundError:
                    raise
                except Exception:
                    stored_bytes = 0
                if (
                    require_bounded_metadata
                    and stored_bytes != declared_file_size
                ):
                    raise RuntimeError("Staging object size disagrees with its metadata")
                if require_bounded_metadata:
                    try:
                        live_metadata = self.storage.stat_object(path)
                        live_identity = live_metadata.identity_token()
                    except Exception as exc:
                        raise RuntimeError(
                            "Staging object identity is unavailable"
                        ) from exc
                    if live_identity != declared_identity:
                        raise RuntimeError(
                            "Staging object identity disagrees with its metadata"
                        )
                    if int(live_metadata.size) != stored_bytes:
                        raise RuntimeError(
                            "Staging object stat disagrees with its byte size"
                        )
                if stored_bytes < 0 or total_bytes + stored_bytes > max_bytes:
                    raise ValueError("Staging read exceeds its byte limit")
                if require_bounded_metadata:
                    import pyarrow.parquet as pq

                    effective_role = self._fresh_role(
                        effective_role, authorization_callback,
                    )
                    check_read_access(
                        super_name=self.super_name,
                        organization=self.organization,
                        role_name=effective_role,
                        table_name=self.super_name,
                        require_unfiltered=True,
                    )
                    temp_path = ""
                    try:
                        with tempfile.NamedTemporaryFile(
                            mode="w+b",
                            prefix="supertable-stage-",
                            suffix=".parquet",
                            delete=False,
                        ) as sink:
                            temp_path = sink.name
                            downloaded = self.storage.download_to_file(
                                path,
                                sink,
                                expected=live_metadata,
                                chunk_size=1024 * 1024,
                            )
                        if downloaded != stored_bytes:
                            raise RuntimeError(
                                "Staging conditional download was incomplete"
                            )
                        parquet = pq.ParquetFile(temp_path)
                        metadata = parquet.metadata
                        if (
                            metadata.num_rows != declared_rows
                            or metadata.num_row_groups > 100_000
                        ):
                            raise RuntimeError(
                                "Staging Parquet metadata disagrees with its index"
                            )
                        expanded = 0
                        for group_index in range(metadata.num_row_groups):
                            group = metadata.row_group(group_index)
                            for column_index in range(group.num_columns):
                                expanded += int(
                                    group.column(
                                        column_index,
                                    ).total_uncompressed_size or 0
                                )
                                if expanded > max_bytes:
                                    raise ValueError(
                                        "Staging Parquet expansion exceeds its memory limit"
                                    )
                        table = pq.read_table(temp_path)
                    finally:
                        if temp_path:
                            try:
                                os.unlink(temp_path)
                            except FileNotFoundError:
                                pass
                else:
                    table = self.storage.read_parquet(path)
                if not isinstance(table, pa.Table):
                    raise RuntimeError("Staging object is not a Parquet table")
                if table.num_columns > max_columns:
                    raise ValueError("Staging schema exceeds its column limit")
                try:
                    schema_bytes = int(table.schema.serialize().size)
                except Exception:
                    schema_bytes = len(str(table.schema).encode("utf-8"))
                if schema_bytes > max_schema_bytes:
                    raise ValueError("Staging schema exceeds its size limit")
                if require_bounded_metadata and (
                    int(table.num_rows) != declared_rows
                    or int(table.num_columns) != declared_columns
                    or int(table.nbytes) != declared_memory
                    or schema_bytes != declared_schema
                ):
                    raise RuntimeError(
                        "Staging object content disagrees with its metadata"
                    )
                total_rows += int(table.num_rows)
                total_bytes += max(stored_bytes, int(table.nbytes))
                if total_rows > max_rows:
                    raise ValueError("Staging read exceeds its row limit")
                if total_bytes > max_bytes:
                    raise ValueError("Staging read exceeds its memory limit")
                tables.append(table)
            return tables

        return self._with_lock(_op)

    def delete_file(
        self,
        role_name: str,
        file_name: str,
        *,
        authorization_callback: Optional[Callable[[], str]] = None,
    ) -> bool:
        """Remove one indexed file without accepting a caller-selected path."""
        stage_name = self._require_stage_mode()
        file_name = _safe_path_component(file_name, label="staging file name")
        check_control_access(
            super_name=self.super_name,
            organization=self.organization,
            role_name=role_name,
            table_name=self.super_name,
        )

        def _op(lock_token: str) -> bool:
            effective_role = self._fresh_role(
                role_name, authorization_callback,
            )
            check_control_access(
                super_name=self.super_name,
                organization=self.organization,
                role_name=effective_role,
                table_name=self.super_name,
            )
            meta, files = self._get_authoritative_stage_files(
                stage_name=stage_name,
                files_index_path=self.files_index_path,
                lock_token=lock_token,
            )
            if meta is None:
                raise FileNotFoundError(f"Staging '{stage_name}' does not exist")
            if file_name not in files:
                raise FileNotFoundError("Staging file is unavailable")
            next_meta = dict(meta)
            next_files = dict(files)
            next_files.pop(file_name)
            next_meta["files"] = next_files
            # Remove discoverability first.  A failed physical delete can leave
            # only an unreferenced object, never a live pointer to missing data.
            self.catalog.upsert_staging_meta(
                self.organization, self.super_name, stage_name,
                meta=next_meta, lock_token=lock_token,
            )
            path = _join_contained(self.stage_dir, file_name)
            try:
                self.storage.delete(path)
            except FileNotFoundError:
                pass
            return True

        return self._with_lock(_op)

    def purge_files(
        self,
        role_name: str,
        *,
        max_files: int = 10_000,
        authorization_callback: Optional[Callable[[], str]] = None,
    ) -> int:
        """Atomically unpublish and then physically remove all staged files."""
        stage_name = self._require_stage_mode()
        check_control_access(
            super_name=self.super_name,
            organization=self.organization,
            role_name=role_name,
            table_name=self.super_name,
        )
        if type(max_files) is not int or max_files <= 0:
            raise ValueError("max_files must be a positive integer")

        def _op(lock_token: str) -> int:
            effective_role = self._fresh_role(
                role_name, authorization_callback,
            )
            check_control_access(
                super_name=self.super_name,
                organization=self.organization,
                role_name=effective_role,
                table_name=self.super_name,
            )
            meta, files = self._get_authoritative_stage_files(
                stage_name=stage_name,
                files_index_path=self.files_index_path,
                lock_token=lock_token,
            )
            if meta is None:
                raise FileNotFoundError(f"Staging '{stage_name}' does not exist")
            if len(files) > max_files:
                raise ValueError("Staging purge exceeds its file limit")
            names = [
                _safe_path_component(name, label="staging file name")
                for name in files
            ]
            next_meta = dict(meta)
            next_meta["files"] = {}
            self.catalog.upsert_staging_meta(
                self.organization, self.super_name, stage_name,
                meta=next_meta, lock_token=lock_token,
            )
            errors = 0
            for name in names:
                try:
                    self.storage.delete(_join_contained(self.stage_dir, name))
                except FileNotFoundError:
                    pass
                except Exception:
                    errors += 1
            if errors:
                raise OSError(
                    f"{errors} unreferenced staging object(s) could not be removed"
                )
            return len(names)

        return self._with_lock(_op)

    def delete(
        self,
        role_name: str,
        *,
        authorization_callback: Optional[Callable[[], str]] = None,
    ) -> str:
        """Start a create-only stage deletion intent and remove the stage."""
        return self._delete_with_intent(
            role_name=role_name,
            authorization_callback=authorization_callback,
        )

    def recover_delete(
            self,
            role_name: str,
            *,
            intent_id: str,
            confirm_previous_owner_stopped: bool = False,
    ) -> str:
        """Resume deletion only after proving the old owner cannot resume."""
        return self._delete_with_intent(
            role_name=role_name,
            recovery_intent_id=intent_id,
            confirm_previous_owner_stopped=confirm_previous_owner_stopped,
        )

    @classmethod
    def recover_pending_delete(
            cls,
            *,
            organization: str,
            super_name: str,
            staging_name: str,
            role_name: str,
            intent_id: str,
            confirm_previous_owner_stopped: bool,
    ) -> str:
        """Recover a fenced stage after the original process has disappeared.

        Normal construction deliberately rejects any live or terminal deletion
        intent.  Recovery therefore builds only the minimal stage shell needed
        to reacquire its lease and delegates to :meth:`recover_delete`, which
        retains the existing CONTROL authorization, exact intent CAS, storage
        cleanup, and explicit prior-owner liveness confirmation.
        """
        identifiers = {
            "organization": organization,
            "super_name": super_name,
            "staging_name": staging_name,
        }
        for label, value in identifiers.items():
            if not isinstance(value, str):
                raise ValueError(f"Invalid {label}: expected a string identifier")
            _safe_path_component(value, label=label)
        # Apply the canonical Redis segment policy as well as filesystem
        # containment validation before constructing any storage/catalog state.
        RK.lock_stage(organization, super_name, staging_name)
        if (
            not isinstance(intent_id, str)
            or not intent_id
            or len(intent_id) > 256
            or "\x00" in intent_id
        ):
            raise ValueError("intent_id must be a non-empty identifier")
        if confirm_previous_owner_stopped is not True:
            raise PermissionError(
                "Deletion recovery requires confirmation that the previous "
                "owner has stopped"
            )

        stage = cls.__new__(cls)
        stage.organization = organization
        stage.super_name = super_name
        stage.storage = get_storage()
        stage.catalog = RedisCatalog()
        stage.base_staging_dir = os.path.join(
            organization, super_name, "staging",
        )
        stage.staging_name = staging_name
        stage._is_manager = False
        stage.stage_dir = _join_contained(
            stage.base_staging_dir, staging_name,
        )
        stage.files_index_path = _join_contained(
            stage.base_staging_dir, f"{staging_name}_files.json",
        )
        return stage.recover_delete(
            role_name,
            intent_id=intent_id,
            confirm_previous_owner_stopped=True,
        )

    def _delete_with_intent(
            self,
            *,
            role_name: str,
            recovery_intent_id: Optional[str] = None,
            confirm_previous_owner_stopped: bool = False,
            authorization_callback: Optional[Callable[[], str]] = None,
    ) -> str:
        stage_name = self._require_stage_mode()
        check_control_access(
            super_name=self.super_name,
            organization=self.organization,
            role_name=role_name,
            table_name=self.super_name,
        )

        def _op(lock_token: str):
            effective_role = self._fresh_role(
                role_name, authorization_callback,
            )
            check_control_access(
                super_name=self.super_name,
                organization=self.organization,
                role_name=effective_role,
                table_name=self.super_name,
            )
            if recovery_intent_id is None:
                intent = self.catalog.begin_stage_deletion(
                    self.organization,
                    self.super_name,
                    stage_name,
                    lock_token=lock_token,
                )
            else:
                intent = self.catalog.recover_stage_deletion(
                    self.organization,
                    self.super_name,
                    stage_name,
                    expected_intent_id=recovery_intent_id,
                    lock_token=lock_token,
                    confirm_previous_owner_stopped=(
                        confirm_previous_owner_stopped
                    ),
                )
            intent_id = intent.get("intent_id") if isinstance(intent, dict) else None
            if not intent_id:
                raise RuntimeError("Catalog returned an invalid staging deletion intent")
            logger.info(
                "[deletion] staging cleanup started for %s/%s/%s; "
                "deletion_intent_id=%s; recovery=%s",
                self.organization,
                self.super_name,
                stage_name,
                intent_id,
                recovery_intent_id is not None,
            )

            # Physical data is deleted and verified before its index/catalog
            # pointers.  If any step fails, metadata remains and a retry can
            # resume without orphaning an undiscoverable prefix.
            self.storage.delete_prefix(self.stage_dir)

            try:
                self.storage.delete(self.files_index_path)
            except FileNotFoundError:
                pass
            if self.storage.exists(self.files_index_path):
                raise OSError(
                    f"Staging index remains after deletion: {self.files_index_path}"
                )

            metadata_removed = self.catalog.delete_staging_meta(
                self.organization,
                self.super_name,
                stage_name,
                lock_token=lock_token,
                intent_id=intent_id,
            )
            if metadata_removed is not True:
                raise RuntimeError(
                    f"Staging metadata removal was incomplete for "
                    f"{self.organization}/{self.super_name}/{stage_name}"
                )
            if recovery_intent_id is not None:
                self.catalog.clear_stage_deletion_tombstone(
                    self.organization,
                    self.super_name,
                    stage_name,
                    expected_intent_id=intent_id,
                    lock_token=lock_token,
                    confirm_previous_owner_stopped=(
                        confirm_previous_owner_stopped
                    ),
                )
            logger.info(
                f"[staging] deleted {stage_name} folder and redis keys; "
                f"deletion_intent_id={intent_id}"
            )
            return str(intent_id)

        return self._with_lock(_op)
