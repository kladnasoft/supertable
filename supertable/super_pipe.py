import time
import uuid
from typing import Any, Dict, List, Optional

from supertable.config.defaults import logger
from supertable.redis_catalog import RedisCatalog


class SuperPipe:
    """
    Redis-only Pipe Management:
    - Validates staging existence in Redis.
    - Stores definitions purely in Redis.
    - Prevents semantic duplicates (simple_name + overwrite_columns).
    """

    def __init__(self, *, organization: str, super_name: str, staging_name: str):
        self.organization = organization
        self.super_name = super_name
        self.staging_name = staging_name
        self.catalog = RedisCatalog()

        # Check if staging exists in Redis before allowing pipe operations
        staging_meta = self.catalog.get_staging_meta(organization, super_name, staging_name)
        if not staging_meta:
            raise FileNotFoundError(f"Staging '{staging_name}' does not exist in Redis for {organization}/{super_name}")

    def _with_lock(self, fn):
        # Lock against the staging area to prevent concurrent pipe/stage mutations
        lock_key = f"supertable:{self.organization}:{self.super_name}:lock:stage:{self.staging_name}"
        token = uuid.uuid4().hex
        acquired = self.catalog.r.set(lock_key, token, nx=True, ex=10)
        if not acquired:
            raise RuntimeError(f"Cannot modify pipes: Stage {self.staging_name} is currently locked.")
        try:
            return fn()
        finally:
            # Simple release
            current_val = self.catalog.r.get(lock_key)
            if current_val and current_val.decode() == token:
                self.catalog.r.delete(lock_key)

    def create(self, *, pipe_name: str, simple_name: str, user_hash: str, overwrite_columns: List[str] = None,
               enabled: bool = True) -> str:
        def _op():
            # 1. Check for duplicate simple_name/overwrite_columns combo
            existing_pipes = self.catalog.list_pipe_metas(self.organization, self.super_name, self.staging_name)
            for p in existing_pipes:
                if p.get("simple_name") == simple_name and p.get("overwrite_columns") == overwrite_columns:
                    if p.get("pipe_name") != pipe_name:
                        raise ValueError(
                            f"A pipe with this simple_name and column configuration already exists: {p.get('pipe_name')}")

            # 3. Define the payload
            definition = {
                "staging_name": self.staging_name,
                "pipe_name": pipe_name,
                "user_hash": user_hash,
                "simple_name": simple_name,
                "overwrite_columns": overwrite_columns or [],
                "transformation": [],
                "updated_at_ns": time.time_ns(),
                "enabled": enabled
            }

            # 4. Save to Redis only
            self.catalog.upsert_pipe_meta(
                self.organization,
                self.super_name,
                self.staging_name,
                pipe_name,
                meta=definition
            )
            logger.info(f"[pipe] created in redis: {pipe_name}")
            return f"redis://{self.organization}/{self.super_name}/{self.staging_name}/{pipe_name}"

        return self._with_lock(_op)

    def set_enabled(self, pipe_name: str, enabled: bool) -> None:
        """Updates the enabled status of a pipe in Redis."""

        def _op():
            meta = self.catalog.get_pipe_meta(self.organization, self.super_name, self.staging_name, pipe_name)
            if not meta:
                raise FileNotFoundError(f"Pipe '{pipe_name}' not found.")

            meta["enabled"] = enabled
            meta["updated_at_ns"] = time.time_ns()

            self.catalog.upsert_pipe_meta(
                self.organization,
                self.super_name,
                self.staging_name,
                pipe_name,
                meta=meta
            )
            logger.info(f"[pipe] updated enabled={enabled} for {pipe_name}")

        return self._with_lock(_op)

    def delete(self, pipe_name: str) -> bool:
        def _op():
            return self.catalog.delete_pipe_meta(
                self.organization,
                self.super_name,
                self.staging_name,
                pipe_name
            ) > 0

        return self._with_lock(_op)

    def read(self, pipe_name: str) -> Dict[str, Any]:
        meta = self.catalog.get_pipe_meta(self.organization, self.super_name, self.staging_name, pipe_name)
        if not meta:
            raise FileNotFoundError(f"Pipe '{pipe_name}' not found.")
        return meta