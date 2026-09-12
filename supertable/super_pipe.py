import time
import uuid
from typing import Any, Dict, List, Optional

from supertable.config.defaults import logger
from supertable.redis_catalog import RedisCatalog
from supertable.rbac.access_control import check_write_access, check_meta_access
from supertable import redis_keys as RK


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
        lock_key = RK.lock_stage(self.organization, self.super_name, self.staging_name)
        token = uuid.uuid4().hex
        acquired = self.catalog.r.set(lock_key, token, nx=True, ex=10)
        if not acquired:
            raise RuntimeError(f"Cannot modify pipes: Stage {self.staging_name} is currently locked.")
        try:
            return fn()
        finally:
            # Atomic release via Lua script (compare-and-delete)
            lua = """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
            """
            self.catalog.r.eval(lua, 1, lock_key, token)

    def create(self, *, role_name: str, pipe_name: str, simple_name: str, user_hash: str, overwrite_columns: List[str] = None,
               enabled: bool = True) -> str:
        """Create or replace a pipe definition.

        ``upsert_pipe_meta`` overwrites unconditionally, so this is an update
        path as well as a create one — and that makes the caller-supplied
        ``simple_name`` insufficient on its own. Checking only it let a role
        with WRITE on ``public`` pass ``pipe_name=<pipe feeding secrets>,
        simple_name=public`` and repoint or destroy a pipe feeding a table it
        had no grant on. When the pipe already exists, WRITE on its *current*
        target is required too: you must be allowed to take it over as well as
        to point it somewhere.
        """
        for table_name in {simple_name, self._pipe_table(pipe_name)}:
            check_write_access(
                super_name=self.super_name,
                organization=self.organization,
                role_name=role_name,
                table_name=table_name,
            )

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

    def _pipe_table(self, pipe_name: str) -> str:
        """The table a pipe feeds, for scoping its access check.

        ``create`` checks against ``simple_name`` — the table the pipe writes
        into — but the other three methods checked against ``self.super_name``,
        looking up the *SuperTable's* name in the role's *table* map. That
        granted pipe control to any role holding a table coincidentally named
        after the lake, and meant a role granted one table could disable or
        delete pipes feeding tables it had no grant on. A pipe's definition
        records its target, so the correct scope was always available.

        A pipe that does not exist falls back to ``"*"``, so only a lake-wide
        grant learns that it is missing; a narrower role is refused instead of
        being told. The caller gets ``FileNotFoundError`` from the operation
        itself once it is past the gate.
        """
        meta = self.catalog.get_pipe_meta(
            self.organization, self.super_name, self.staging_name, pipe_name,
        )
        return (meta or {}).get("simple_name") or "*"

    def set_enabled(self, pipe_name: str, enabled: bool, role_name: str) -> None:
        """Updates the enabled status of a pipe in Redis."""
        check_write_access(
            super_name=self.super_name,
            organization=self.organization,
            role_name=role_name,
            table_name=self._pipe_table(pipe_name),
        )

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

    def delete(self, pipe_name: str, role_name: str) -> bool:
        check_write_access(
            super_name=self.super_name,
            organization=self.organization,
            role_name=role_name,
            table_name=self._pipe_table(pipe_name),
        )

        def _op():
            return self.catalog.delete_pipe_meta(
                self.organization,
                self.super_name,
                self.staging_name,
                pipe_name
            ) > 0

        return self._with_lock(_op)

    def read(self, pipe_name: str, role_name: str) -> Dict[str, Any]:
        check_meta_access(
            super_name=self.super_name,
            organization=self.organization,
            role_name=role_name,
            table_name=self._pipe_table(pipe_name),
        )
        meta = self.catalog.get_pipe_meta(self.organization, self.super_name, self.staging_name, pipe_name)
        if not meta:
            raise FileNotFoundError(f"Pipe '{pipe_name}' not found.")
        return meta