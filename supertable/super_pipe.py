import time
from typing import Any, Dict, List, Optional

from supertable.config.defaults import logger
from supertable.redis_catalog import RedisCatalog
from supertable.rbac.access_control import check_write_access, check_meta_access


_GENERIC_PIPE_DENIAL = "Pipe is unavailable under the effective access policy."


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

        deletion_guard = getattr(
            type(self.catalog), "check_deletion_intent_absent", None,
        )
        if callable(deletion_guard):
            self.catalog.check_deletion_intent_absent(
                self.organization,
                self.super_name,
                stage=self.staging_name,
            )

        # Check if staging exists in Redis before allowing pipe operations
        staging_meta = self.catalog.get_staging_meta(organization, super_name, staging_name)
        if not staging_meta:
            raise FileNotFoundError(f"Staging '{staging_name}' does not exist in Redis for {organization}/{super_name}")

    def _with_lock(self, fn):
        # Lock against the staging area to prevent concurrent pipe/stage mutations
        token = self.catalog.acquire_stage_lock(
            self.organization,
            self.super_name,
            self.staging_name,
            ttl_s=30,
            timeout_s=30,
        )
        if not token:
            raise RuntimeError(f"Cannot modify pipes: Stage {self.staging_name} is currently locked.")
        try:
            return fn(token)
        finally:
            self.catalog.release_stage_lock(
                self.organization,
                self.super_name,
                self.staging_name,
                token,
            )

    def _check_target_access(
            self, *, role_name: str, target: str, mutation: bool,
    ) -> None:
        try:
            if mutation:
                check_write_access(
                    super_name=self.super_name,
                    organization=self.organization,
                    role_name=role_name,
                    table_name=target,
                )
            else:
                check_meta_access(
                    super_name=self.super_name,
                    organization=self.organization,
                    role_name=role_name,
                    table_name=target,
                )
        except PermissionError:
            # Table-specific RBAC errors include the stored target name. A pipe
            # identifier must not become an oracle for that protected value.
            raise PermissionError(_GENERIC_PIPE_DENIAL) from None

    def _may_disclose_pipe_state(
            self, *, role_name: str, mutation: bool,
    ) -> bool:
        """Whether a caller has namespace-wide access to pipe diagnostics."""
        try:
            if mutation:
                check_write_access(
                    super_name=self.super_name,
                    organization=self.organization,
                    role_name=role_name,
                    table_name=self.super_name,
                )
            else:
                check_meta_access(
                    super_name=self.super_name,
                    organization=self.organization,
                    role_name=role_name,
                    table_name=self.super_name,
                )
        except PermissionError:
            return False
        return True

    def _load_authorized_pipe(
            self, *, pipe_name: str, role_name: str, mutation: bool,
    ) -> Dict[str, Any]:
        """Load a definition without exposing state before target RBAC passes."""
        try:
            meta = self.catalog.get_pipe_meta(
                self.organization, self.super_name, self.staging_name,
                pipe_name,
            )
        except RuntimeError:
            if self._may_disclose_pipe_state(
                role_name=role_name, mutation=mutation,
            ):
                raise
            raise PermissionError(_GENERIC_PIPE_DENIAL) from None

        if not meta:
            if self._may_disclose_pipe_state(
                role_name=role_name, mutation=mutation,
            ):
                raise FileNotFoundError(f"Pipe '{pipe_name}' not found.")
            raise PermissionError(_GENERIC_PIPE_DENIAL)

        target = meta.get("simple_name")
        if not isinstance(target, str) or not target:
            if self._may_disclose_pipe_state(
                role_name=role_name, mutation=mutation,
            ):
                raise RuntimeError(
                    f"Pipe '{pipe_name}' has invalid target metadata."
                )
            raise PermissionError(_GENERIC_PIPE_DENIAL)

        self._check_target_access(
            role_name=role_name, target=target, mutation=mutation,
        )

        # A long-lived SuperPipe can outlive the stage it was constructed
        # against.  Recheck both namespace and stage tombstones on every read
        # as well as every mutation.  This happens only after target RBAC, so a
        # scoped caller still receives the same generic denial for live and
        # deleting protected pipes.
        deletion_guard = getattr(
            type(self.catalog), "check_deletion_intent_absent", None,
        )
        if callable(deletion_guard):
            try:
                self.catalog.check_deletion_intent_absent(
                    self.organization,
                    self.super_name,
                    stage=self.staging_name,
                )
            except RuntimeError:
                if self._may_disclose_pipe_state(
                    role_name=role_name, mutation=mutation,
                ):
                    raise
                raise PermissionError(_GENERIC_PIPE_DENIAL) from None
        return meta

    def create(self, *, role_name: str, pipe_name: str, simple_name: str, user_hash: str, overwrite_columns: List[str] = None,
               enabled: bool = True) -> str:
        def _op(lock_token: str):
            # Authorize the caller-supplied target before inspecting any stored
            # pipe name. Unauthorized creation attempts therefore cannot probe
            # the pipe namespace at all.
            self._check_target_access(
                role_name=role_name, target=simple_name, mutation=True,
            )
            try:
                existing = self.catalog.get_pipe_meta(
                    self.organization, self.super_name, self.staging_name,
                    pipe_name,
                )
            except RuntimeError:
                if self._may_disclose_pipe_state(
                    role_name=role_name, mutation=True,
                ):
                    raise
                raise PermissionError(_GENERIC_PIPE_DENIAL) from None
            if existing:
                existing_target = existing.get("simple_name")
                if not isinstance(existing_target, str) or not existing_target:
                    if self._may_disclose_pipe_state(
                        role_name=role_name, mutation=True,
                    ):
                        raise RuntimeError(
                            f"Pipe '{pipe_name}' has invalid target metadata."
                        )
                    raise PermissionError(_GENERIC_PIPE_DENIAL)
                # Do not disclose or replace a pipe belonging to a table the
                # caller cannot mutate. Pipe creation is create-only; explicit
                # lifecycle methods are the only supported mutations.
                self._check_target_access(
                    role_name=role_name,
                    target=existing_target,
                    mutation=True,
                )
                raise FileExistsError(f"Pipe '{pipe_name}' already exists.")

            # 1. Check for duplicate simple_name/overwrite_columns combo
            try:
                existing_pipes = self.catalog.list_pipe_metas(
                    self.organization, self.super_name, self.staging_name,
                )
            except RuntimeError:
                if self._may_disclose_pipe_state(
                    role_name=role_name, mutation=True,
                ):
                    raise
                raise PermissionError(_GENERIC_PIPE_DENIAL) from None
            requested_overwrite = list(overwrite_columns or [])
            for p in existing_pipes:
                if (
                    p.get("simple_name") == simple_name
                    and list(p.get("overwrite_columns") or []) == requested_overwrite
                ):
                    raise ValueError(
                        "A pipe with this simple_name and column configuration "
                        f"already exists: {p.get('pipe_name')}"
                    )

            # 3. Define the payload
            definition = {
                "staging_name": self.staging_name,
                "pipe_name": pipe_name,
                "user_hash": user_hash,
                "simple_name": simple_name,
                "overwrite_columns": requested_overwrite,
                "transformation": [],
                "updated_at_ns": time.time_ns(),
                "enabled": enabled
            }

            # 4. Save to Redis only
            try:
                self.catalog.upsert_pipe_meta(
                    self.organization,
                    self.super_name,
                    self.staging_name,
                    pipe_name,
                    meta=definition,
                    lock_token=lock_token,
                    create_only=True,
                )
            except FileExistsError:
                # A competing creator may have published a definition for a
                # different protected target after our read. Do not disclose
                # that race winner without re-authorizing its exact metadata.
                raise PermissionError(_GENERIC_PIPE_DENIAL) from None
            logger.info(f"[pipe] created in redis: {pipe_name}")
            return f"redis://{self.organization}/{self.super_name}/{self.staging_name}/{pipe_name}"

        return self._with_lock(_op)

    def set_enabled(self, pipe_name: str, enabled: bool, role_name: str) -> None:
        """Updates the enabled status of a pipe in Redis."""
        def _op(lock_token: str):
            meta = self._load_authorized_pipe(
                pipe_name=pipe_name, role_name=role_name, mutation=True,
            )

            meta["enabled"] = enabled
            meta["updated_at_ns"] = time.time_ns()

            self.catalog.upsert_pipe_meta(
                self.organization,
                self.super_name,
                self.staging_name,
                pipe_name,
                meta=meta,
                lock_token=lock_token,
            )
            logger.info(f"[pipe] updated enabled={enabled} for {pipe_name}")

        return self._with_lock(_op)

    def delete(self, pipe_name: str, role_name: str) -> bool:
        def _op(lock_token: str):
            self._load_authorized_pipe(
                pipe_name=pipe_name, role_name=role_name, mutation=True,
            )
            return self.catalog.delete_pipe_meta(
                self.organization,
                self.super_name,
                self.staging_name,
                pipe_name,
                lock_token=lock_token,
            ) > 0

        return self._with_lock(_op)

    def read(self, pipe_name: str, role_name: str) -> Dict[str, Any]:
        return self._load_authorized_pipe(
            pipe_name=pipe_name, role_name=role_name, mutation=False,
        )
