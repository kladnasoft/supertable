# supertable/staging_area.py
from __future__ import annotations

import os
import time
import uuid
from typing import Any, Dict, List, Optional

import pyarrow as pa

from supertable.config.defaults import logger
from supertable import redis_keys as RK
from supertable.storage.storage_factory import get_storage
from supertable.redis_catalog import DeletionIntentConflictError, RedisCatalog
from supertable.rbac.access_control import (
    check_control_access,
    check_create_access,
    check_meta_access,
    check_write_access,
)


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
    ):
        raise ValueError(f"Invalid {label}: expected one non-empty path component")
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
            self.staging_name = _safe_path_component(
                self.staging_name, label="staging_name",
            )
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

    def get_directory_structure(self, role_name: str) -> Dict[str, Any]:
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
        stagings = self.catalog.list_stagings(self.organization, self.super_name)

        stages: List[Dict[str, Any]] = []
        for name in sorted(stagings):
            try:
                safe_name = _safe_path_component(name, label="stored staging_name")
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
        stage_name = _safe_path_component(stage_name, label="staging_name")
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
        stage_name = _safe_path_component(stage_name, label="staging_name")

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

    def save_as_parquet(self, *, role_name: str, arrow_table: pa.Table, base_file_name: str,
                        source: str = "upload", duration_ms: float = 0, pipe_name: str = "", pipe_id: str = "") -> str:
        stage_name = self._require_stage_mode()
        def _op(_lock_token: str):
            existing_meta = self.catalog.get_staging_meta(
                self.organization, self.super_name, stage_name,
            )
            access = check_write_access if existing_meta is not None else check_create_access
            access(
                super_name=self.super_name,
                organization=self.organization,
                role_name=role_name,
                table_name=self.super_name,
            )
            if existing_meta is not None:
                existing_meta, _ = self._get_authoritative_stage_files(
                    stage_name=stage_name,
                    files_index_path=self.files_index_path,
                    lock_token=_lock_token,
                    known_meta=existing_meta,
                )
            initial_meta = self._init_stage(
                _lock_token, existing_meta=existing_meta,
            )
            ts_ns = time.time_ns()
            # The caller-supplied display name is metadata only.  Never let it
            # select a filesystem/object key; a server-generated component also
            # prevents collisions between concurrent/retried uploads.
            file_name = f"stage_{ts_ns}_{uuid.uuid4().hex}.parquet"
            file_path = _join_contained(self.stage_dir, file_name)

            self.storage.write_parquet(arrow_table, file_path)

            file_meta = {
                "file": file_name, "written_at_ns": ts_ns,
                "rows": arrow_table.num_rows, "source": source,
                "duration_ms": round(duration_ms) if duration_ms else None,
                "pipe_name": pipe_name or None, "pipe_id": pipe_id or None,
                "status": "ok",
                "original_name": os.path.basename(str(base_file_name or "")) or None,
            }
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

    def delete(self, role_name: str) -> str:
        """Start a create-only stage deletion intent and remove the stage."""
        return self._delete_with_intent(role_name=role_name)

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
    ) -> str:
        stage_name = self._require_stage_mode()
        check_control_access(
            super_name=self.super_name,
            organization=self.organization,
            role_name=role_name,
            table_name=self.super_name,
        )

        def _op(lock_token: str):
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
