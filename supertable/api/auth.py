from __future__ import annotations

import os

from fastapi import HTTPException, Request, status


def _must_get_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


class AuthConfig:
    def __init__(self) -> None:
        self.mode = os.getenv("SUPERTABLE_AUTH_MODE", "api_key").strip().lower()
        self.api_key_header_name = os.getenv("SUPERTABLE_AUTH_HEADER_NAME", "X-API-Key").strip()

        if self.mode not in ("api_key", "bearer"):
            raise RuntimeError(
                "SUPERTABLE_AUTH_MODE must be one of: api_key | bearer "
                f"(got: {self.mode})"
            )

        if self.mode == "api_key":
            self.api_key = _must_get_env("SUPERTABLE_API_KEY")
            self.bearer_token = ""
        else:
            self.bearer_token = _must_get_env("SUPERTABLE_BEARER_TOKEN")
            self.api_key = ""

    def is_excluded(self, path: str) -> bool:
        # Hard-coded security rule: ONLY healthz is excluded
        return path == "/healthz"


def build_auth_dependency():
    """
    Enforces either API key or Bearer token authentication.
    Security design: only /healthz is excluded (hardcoded, not configurable).
    """
    cfg = AuthConfig()

    async def _auth_guard(request: Request) -> None:
        if cfg.is_excluded(request.url.path):
            return

        if cfg.mode == "api_key":
            presented = request.headers.get(cfg.api_key_header_name, "")
            if not presented or presented.strip() != cfg.api_key:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid or missing API key",
                )
            return

        # bearer mode
        auth = request.headers.get("Authorization", "")
        if not auth:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing Authorization header",
            )

        prefix = "bearer "
        if not auth.lower().startswith(prefix):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authorization must be: Bearer <token>",
            )

        token = auth[len(prefix):].strip()
        if token != cfg.bearer_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid bearer token",
            )

    return _auth_guard
