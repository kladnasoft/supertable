import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from supertable.config.defaults import logger


@dataclass(frozen=True)
class SuperPipeDefinition:
    pipe_name: str
    organization: str
    super_name: str
    staging_name: str
    user_hash: str
    simple_name: str
    overwrite_columns: List[str]
    enabled: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipe_name": self.pipe_name,
            "organization": self.organization,
            "super_name": self.super_name,
            "staging_name": self.staging_name,
            "user_hash": self.user_hash,
            "simple_name": self.simple_name,
            "overwrite_columns": self.overwrite_columns,
            "enabled": self.enabled,
            "updated_at_ns": time.time_ns(),
        }


class SuperPipe:
    """Snowpipe-like loader configuration stored in *storage* under a staging.

    Storage layout (under an existing supertable):
        <org>/<super>/staging/<staging_name>/pipes/<pipe_name>.json

    Notes:
    - Definitions are stored in storage (not Redis).
    - Locks/deduplication should be handled elsewhere (Redis), per your design.
    """

    def __init__(self, *, organization: str, super_name: str, staging_name: str):
        self.organization = organization
        self.super_name = super_name
        self.staging_name = staging_name
        self.identity = "staging"

        from supertable.storage.storage_factory import get_storage  # noqa: WPS433
        from supertable.redis_catalog import RedisCatalog  # noqa: WPS433

        self.storage = get_storage()
        self.catalog = RedisCatalog()

        # Validate supertable exists (do not create).
        if not self.catalog.root_exists(self.organization, self.super_name):
            raise FileNotFoundError(
                f"SuperTable does not exist: organization={self.organization!r}, super_name={self.super_name!r}"
            )

        self.staging_dir = os.path.join(self.organization, self.super_name, self.identity, self.staging_name)
        self.pipes_dir = os.path.join(self.staging_dir, "pipes")

        if not self.storage.exists(self.pipes_dir):
            # Best-effort: staging init may not have run yet
            self.storage.makedirs(self.pipes_dir)


    def _list_pipe_definition_paths(self) -> List[str]:
        """Return all pipe definition JSON paths under this staging's pipes directory."""
        try:
            tree = self.storage.get_directory_structure(self.pipes_dir)
        except Exception:
            # If the storage backend can't list, fall back to empty (no duplicate detection)
            return []

        paths: List[str] = []

        def dfs(prefix: str, node) -> None:
            if node is None:
                if prefix.endswith(".json"):
                    paths.append(prefix)
                return
            if not isinstance(node, dict):
                return
            for name, child in node.items():
                dfs(os.path.join(prefix, name), child)

        dfs(self.pipes_dir, tree)
        return paths

    def _find_duplicate_pipe(
        self,
        *,
        pipe_name: str,
        user_hash: str,
        simple_name: str,
        overwrite_columns: List[str],
    ) -> Optional[Dict[str, Any]]:
        """Detect an existing pipe with the same semantics but a different name.

        We treat a pipe as "the same" if it targets the same:
          - organization/super_name/staging_name (this instance)
          - user_hash
          - simple_name
          - overwrite_columns (exact list match)

        Returns:
            dict with keys: pipe_name, path if a duplicate is found; otherwise None.
        """
        for path in self._list_pipe_definition_paths():
            try:
                data = self.storage.read_json(path)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue

            existing_name = data.get("pipe_name")
            if existing_name == pipe_name:
                # same name is handled by the caller via exists(path)
                continue

            if data.get("organization") != self.organization:
                continue
            if data.get("super_name") != self.super_name:
                continue
            if data.get("staging_name") != self.staging_name:
                continue
            if data.get("user_hash") != user_hash:
                continue
            if data.get("simple_name") != simple_name:
                continue

            existing_overwrite = data.get("overwrite_columns") or []
            if list(existing_overwrite) != list(overwrite_columns):
                continue

            return {"pipe_name": existing_name, "path": path}

        return None

    def _pipe_path(self, pipe_name: str) -> str:
        return os.path.join(self.pipes_dir, f"{pipe_name}.json")

    def create(
        self,
        *,
        pipe_name: str,
        user_hash: str,
        simple_name: str,
        overwrite_columns: Optional[List[str]] = None,
        enabled: bool = True,
    ) -> str:
        """Create a new pipe definition (must be unique). Returns the storage path."""
        overwrite_columns = overwrite_columns or []
        path = self._pipe_path(pipe_name)

        dup = self._find_duplicate_pipe(
            pipe_name=pipe_name,
            user_hash=user_hash,
            simple_name=simple_name,
            overwrite_columns=overwrite_columns,
        )
        if dup:
            raise ValueError(
                "Pipe already exists with different name: "
                f"requested={pipe_name!r}, existing={dup.get('pipe_name')!r} at {dup.get('path')}"
            )

        if self.storage.exists(path):
            raise FileExistsError(f"Pipe already exists: {pipe_name!r} at {path}")

        definition = SuperPipeDefinition(
            pipe_name=pipe_name,
            organization=self.organization,
            super_name=self.super_name,
            staging_name=self.staging_name,
            user_hash=user_hash,
            simple_name=simple_name,
            overwrite_columns=overwrite_columns,
            enabled=enabled,
        )

        self.storage.write_json(path, definition.to_dict())
        logger.info(f"[pipe] created: {pipe_name} -> {path}")
        return path

    def set_enabled(self, *, pipe_name: str, enabled: bool) -> None:
        path = self._pipe_path(pipe_name)
        if not self.storage.exists(path):
            raise FileNotFoundError(f"Pipe not found: {pipe_name!r}")

        data = self.storage.read_json(path)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid pipe definition at {path}")

        data["enabled"] = bool(enabled)
        data["updated_at_ns"] = time.time_ns()
        self.storage.write_json(path, data)
        logger.info(f"[pipe] updated enabled={enabled} for {pipe_name}")

    def read(self, *, pipe_name: str) -> Dict[str, Any]:
        path = self._pipe_path(pipe_name)
        if not self.storage.exists(path):
            raise FileNotFoundError(f"Pipe not found: {pipe_name!r}")
        data = self.storage.read_json(path)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid pipe definition at {path}")
        return data
