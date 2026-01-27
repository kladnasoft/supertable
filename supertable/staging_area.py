from __future__ import annotations

import os
import json
import time
import uuid
from typing import List, Dict, Any, Optional

import pyarrow as pa

from supertable.config.defaults import logger
from supertable.storage.storage_factory import get_storage
from supertable.redis_catalog import RedisCatalog


class Staging:
    """
    Staging implementation:
    - Creates staging folder and registers in Redis.
    - Maintains a {staging_name}_files.json index in the parent 'staging' folder.
    - Uses a Redis lock for all write/delete operations.
    """

    def __init__(self, *, organization: str, super_name: str, staging_name: str):
        self.organization = organization
        self.super_name = super_name
        self.staging_name = staging_name

        self.storage = get_storage()
        self.catalog = RedisCatalog()

        # Validate supertable existence
        if not self.catalog.root_exists(organization, super_name):
            raise FileNotFoundError(f"SuperTable does not exist: {organization}/{super_name}")

        self.base_staging_dir = os.path.join(organization, super_name, "staging")
        self.stage_dir = os.path.join(self.base_staging_dir, staging_name)
        # Index file is at staging/{staging_name}_files.json
        self.files_index_path = os.path.join(self.base_staging_dir, f"{staging_name}_files.json")

        self._with_lock(self._init_stage)

    def _with_lock(self, fn):
        # Using the standard lock key format requested
        lock_key = f"supertable:{self.organization}:{self.super_name}:lock:stage:{self.staging_name}"
        token = uuid.uuid4().hex

        # Acquire lock (30s TTL, 30s timeout)
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
        # 1. Create the physical folder (staging/{staging_name})
        if not self.storage.exists(self.stage_dir):
            self.storage.makedirs(self.stage_dir)

        # 2. Register in Redis (No _staging.json created)
        self.catalog.upsert_staging_meta(
            self.organization,
            self.super_name,
            self.staging_name,
            meta={"path": self.stage_dir, "created_at_ms": int(time.time() * 1000)},
        )

        # 3. Ensure files index exists in the parent 'staging' folder
        if not self.storage.exists(self.files_index_path):
            self.storage.write_json(self.files_index_path, [])

    def save_as_parquet(self, *, arrow_table: pa.Table, base_file_name: str) -> str:
        def _op():
            ts_ns = time.time_ns()
            clean_name = base_file_name.rsplit(".parquet", 1)[0]
            file_name = f"{clean_name}_{ts_ns}.parquet"
            # Path: staging/{stage_name}/{file_name}
            file_path = os.path.join(self.stage_dir, file_name)

            # Write Parquet
            self.storage.write_parquet(arrow_table, file_path)

            # Update flat JSON index (staging/{stage_name}_files.json)
            current_index = self.storage.read_json(self.files_index_path) or []
            current_index.append({
                "file": file_name,
                "written_at_ns": ts_ns,
                "rows": arrow_table.num_rows,
            })
            self.storage.write_json(self.files_index_path, current_index)

            logger.info(f"[staging] saved {file_name} and updated index at {self.files_index_path}")
            return file_name

        return self._with_lock(_op)

    def list_files(self) -> List[str]:
        """Reads the index file instead of scanning the directory."""
        if not self.storage.exists(self.files_index_path):
            return []
        data = self.storage.read_json(self.files_index_path) or []
        return [item.get("file") for item in data if isinstance(item, dict)]

    def delete(self) -> None:
        def _op():
            # Delete physical stage folder
            if self.storage.exists(self.stage_dir):
                self.storage.delete_recursive(self.stage_dir)

            # Delete the index file in parent directory
            if self.storage.exists(self.files_index_path):
                self.storage.delete(self.files_index_path)

            # Wipe Redis meta (deletes the staging key, the meta index, and all associated pipes)
            self.catalog.delete_staging_meta(
                self.organization,
                self.super_name,
                self.staging_name,
            )
            logger.info(f"[staging] deleted {self.staging_name} folder, index, and redis keys")

        self._with_lock(_op)