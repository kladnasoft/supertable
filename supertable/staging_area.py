# supertable/staging_area.py
from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pyarrow as pa

from supertable.config.defaults import logger
from supertable.storage.storage_factory import get_storage
from supertable.redis_catalog import RedisCatalog


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


@dataclass(frozen=True)
class StageInfo:
    name: str
    path: str
    files_index_path: str
    files: List[str]
    file_count: int


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
        supertable:{org}:{sup}:meta:staging:{stage_name}
        plus an index set for fast listing.
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

        # Validate supertable existence
        if not self.catalog.root_exists(self.organization, self.super_name):
            raise FileNotFoundError(f"SuperTable does not exist: {self.organization}/{self.super_name}")

        self.base_staging_dir = os.path.join(self.organization, self.super_name, "staging")

        # Mode
        self.staging_name = staging_name
        self._is_manager = staging_name is None

        if not self._is_manager:
            # Stage mode paths
            self.stage_dir = os.path.join(self.base_staging_dir, self.staging_name)  # type: ignore[arg-type]
            self.files_index_path = os.path.join(self.base_staging_dir, f"{self.staging_name}_files.json")
            self._with_lock(self._init_stage)
        else:
            # Manager mode doesn't create anything; just sets placeholders.
            self.stage_dir = None
            self.files_index_path = None

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

    def get_directory_structure(self) -> Dict[str, Any]:
        """
        Backwards-compatible method used by examples/3.4. read_staging.py.

        Returns a JSON-serializable dict describing the staging area and stages.
        """
        # Ensure base exists (read-only; no lock)
        base_exists = self.storage.exists(self.base_staging_dir)
        stagings = self.catalog.list_stagings(self.organization, self.super_name)

        stages: List[Dict[str, Any]] = []
        for name in sorted(stagings):
            stage_dir = os.path.join(self.base_staging_dir, name)
            files_index_path = os.path.join(self.base_staging_dir, f"{name}_files.json")

            files: List[str] = []
            if self.storage.exists(files_index_path):
                try:
                    data = self.storage.read_json(files_index_path) or []
                    files = [it.get("file") for it in data if isinstance(it, dict) and it.get("file")]
                except Exception as e:
                    logger.warning(f"[staging] failed reading index {files_index_path}: {e}")

            stages.append(
                {
                    "name": name,
                    "path": stage_dir,
                    "exists": bool(self.storage.exists(stage_dir)),
                    "files_index_path": files_index_path,
                    "files_index_exists": bool(self.storage.exists(files_index_path)),
                    "file_count": len(files),
                    "files": files,
                    "redis_meta": self.catalog.get_staging_meta(self.organization, self.super_name, name) or {},
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

    def _require_stage_mode(self) -> None:
        if self._is_manager:
            raise RuntimeError(
                "This Staging instance is in manager mode (no staging_name was provided). "
                "Call .open(staging_name) first."
            )

    def _with_lock(self, fn):
        self._require_stage_mode()
        lock_key = f"supertable:{self.organization}:{self.super_name}:lock:stage:{self.staging_name}"
        token = uuid.uuid4().hex

        # Acquire lock (30s TTL)
        acquired = self.catalog.r.set(lock_key, token, nx=True, ex=30)
        if not acquired:
            raise RuntimeError(f"Stage {self.staging_name} is currently locked by another process.")

        try:
            return fn()
        finally:
            # Release lock via Lua script
            lua = """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
            """
            self.catalog.r.eval(lua, 1, lock_key, token)

    def _init_stage(self) -> None:
        self._require_stage_mode()

        # 1) Create the physical folder (staging/{staging_name})
        if not self.storage.exists(self.stage_dir):
            self.storage.makedirs(self.stage_dir)

        # 2) Register in Redis (No _staging.json created)
        self.catalog.upsert_staging_meta(
            self.organization,
            self.super_name,
            self.staging_name,  # type: ignore[arg-type]
            meta={"path": self.stage_dir, "created_at_ms": int(time.time() * 1000)},
        )

        # 3) Ensure files index exists in the parent 'staging' folder
        if not self.storage.exists(self.files_index_path):
            self.storage.write_json(self.files_index_path, [])

    def save_as_parquet(self, *, arrow_table: pa.Table, base_file_name: str) -> str:
        self._require_stage_mode()

        def _op():
            ts_ns = time.time_ns()
            clean_name = base_file_name.rsplit(".parquet", 1)[0]
            file_name = f"{clean_name}_{ts_ns}.parquet"
            file_path = os.path.join(self.stage_dir, file_name)

            self.storage.write_parquet(arrow_table, file_path)

            current_index = self.storage.read_json(self.files_index_path) or []
            current_index.append(
                {"file": file_name, "written_at_ns": ts_ns, "rows": arrow_table.num_rows}
            )
            self.storage.write_json(self.files_index_path, current_index)

            logger.info(f"[staging] saved {file_name} and updated index at {self.files_index_path}")
            return file_name

        return self._with_lock(_op)

    def list_files(self) -> List[str]:
        self._require_stage_mode()
        if not self.storage.exists(self.files_index_path):
            return []
        data = self.storage.read_json(self.files_index_path) or []
        return [item.get("file") for item in data if isinstance(item, dict) and item.get("file")]

    def delete(self) -> None:
        self._require_stage_mode()

        def _op():
            if self.storage.exists(self.stage_dir):
                self.storage.delete_recursive(self.stage_dir)

            if self.storage.exists(self.files_index_path):
                self.storage.delete(self.files_index_path)

            self.catalog.delete_staging_meta(
                self.organization,
                self.super_name,
                self.staging_name,  # type: ignore[arg-type]
            )
            logger.info(f"[staging] deleted {self.staging_name} folder, index, and redis keys")

        self._with_lock(_op)
