# path: supertable/reflection/users.py
"""
Route module for the standalone Users & Tokens page.

Registers:
  GET    /reflection/users-page              — renders users.html
  GET    /reflection/users-page/list         — API: list users (JSON, for dynamic refresh)
  POST   /reflection/users-page/create       — API: create user via UserManager
  DELETE /reflection/users-page/user/{uid}   — API: delete user
  POST   /reflection/users-page/user/{uid}/role         — API: add role to user
  DELETE /reflection/users-page/user/{uid}/role/{rid}   — API: remove role from user
  GET    /reflection/user/{user_id}          — API: user details
  GET    /reflection/role/{role_id}          — API: role details
  GET    /reflection/tokens                  — API: list tokens
  POST   /reflection/tokens                  — API: create token
  DELETE /reflection/tokens/{token_id}       — API: delete token
"""
from __future__ import annotations

import hashlib
import json
import logging
import secrets
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

logger = logging.getLogger(__name__)

# Valid role types (mirrors permissions.py RoleType)
_VALID_ROLE_TYPES = {"superadmin", "admin", "writer", "reader", "meta"}


def attach_users_routes(
    router: APIRouter,
    *,
    templates: Any,
    settings: Any,
    redis_client: Any,
    catalog: Any,
    is_authorized: Callable[[Request], bool],
    no_store: Callable[[Response], None],
    get_provided_token: Callable[[Request], Optional[str]],
    discover_pairs: Callable[[], List[Tuple[str, str]]],
    resolve_pair: Callable[[Optional[str], Optional[str]], Tuple[str, str]],
    inject_session_into_ctx: Callable[[Dict[str, Any], Request], Any],
    list_users: Callable[[str, str], List[Dict[str, Any]]],
    list_roles: Callable[[str, str], List[Dict[str, Any]]],
    read_user: Callable[[str, str, str], Optional[Dict[str, Any]]],
    read_role: Callable[[str, str, str], Optional[Dict[str, Any]]],
    logged_in_guard_api: Any,
    admin_guard_api: Any,
) -> None:
    """Attach /reflection/users-page and supporting API routes to router."""

    # ── UserManager factory ───────────────────────────────────────────────────

    def _user_manager(org: str, sup: str):
        """Instantiate UserManager with the shared RedisCatalog."""
        try:
            from supertable.rbac.user_manager import UserManager  # type: ignore
            return UserManager(super_name=sup, organization=org, redis_catalog=catalog)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"UserManager unavailable: {e}")

    def _role_manager(org: str, sup: str):
        """Instantiate RoleManager with the shared RedisCatalog."""
        try:
            from supertable.rbac.role_manager import RoleManager  # type: ignore
            return RoleManager(super_name=sup, organization=org, redis_catalog=catalog)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"RoleManager unavailable: {e}")

    # ── Token helpers ─────────────────────────────────────────────────────────

    def _get_org_from_env_fallback() -> str:
        return (settings.SUPERTABLE_ORGANIZATION or "").strip()

    def _catalog_list_tokens(org: str) -> List[Dict[str, Any]]:
        if not org:
            return []
        try:
            return catalog.list_auth_tokens(org)
        except Exception:
            try:
                raw = redis_client.hgetall(f"supertable:{org}:auth:tokens") or {}
                out: List[Dict[str, Any]] = []
                for token_id, meta_raw in raw.items():
                    try:
                        meta = json.loads(meta_raw) if meta_raw else {}
                    except Exception:
                        meta = {"value": meta_raw}
                    meta = dict(meta) if isinstance(meta, dict) else {"value": meta}
                    meta.setdefault("token_id", token_id)
                    out.append(meta)
                out.sort(key=lambda x: int(x.get("created_ms") or 0), reverse=True)
                return out
            except Exception:
                return []

    def _catalog_create_token(
        org: str, created_by: str, label: Optional[str]
    ) -> Dict[str, Any]:
        if not org:
            raise HTTPException(status_code=400, detail="Missing organization")
        try:
            return catalog.create_auth_token(org=org, created_by=created_by, label=label)
        except Exception:
            token = secrets.token_urlsafe(24)
            token_id = hashlib.sha256(token.encode("utf-8")).hexdigest()
            meta = {
                "token_id": token_id,
                "created_ms": int(time.time() * 1000),
                "created_by": str(created_by or ""),
                "label": (str(label).strip() if label is not None else ""),
                "enabled": True,
            }
            try:
                redis_client.hset(
                    f"supertable:{org}:auth:tokens",
                    token_id,
                    json.dumps(meta),
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Token creation failed: {e}")
            return {"token": token, **meta}

    def _catalog_delete_token(org: str, token_id: str) -> bool:
        if not org or not token_id:
            return False
        try:
            return bool(catalog.delete_auth_token(org=org, token_id=token_id))
        except Exception:
            try:
                return bool(
                    redis_client.hdel(f"supertable:{org}:auth:tokens", token_id)
                )
            except Exception:
                return False

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _resolve_or_raise(org: Optional[str], sup: Optional[str]) -> Tuple[str, str]:
        o, s = resolve_pair(org, sup)
        if not o or not s:
            raise HTTPException(status_code=400, detail="No tenant selected")
        return o, s

    def _normalize_user(u: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure user_id and hash are always set."""
        uid = u.get("user_id") or u.get("hash") or ""
        u.setdefault("user_id", uid)
        u.setdefault("hash", uid)
        roles = u.get("roles") or []
        if isinstance(roles, str):
            try:
                roles = json.loads(roles)
            except Exception:
                roles = [roles]
        u["roles"] = roles
        return u

    # ── HTML page ─────────────────────────────────────────────────────────────

    @router.get("/reflection/users-page", response_class=HTMLResponse)
    def users_page(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
    ):
        if not is_authorized(request):
            resp = RedirectResponse("/reflection/login", status_code=302)
            no_store(resp)
            return resp

        provided = get_provided_token(request) or ""
        pairs = discover_pairs()
        sel_org, sel_sup = resolve_pair(org, sup)
        tenants = [
            {"org": o, "sup": s, "selected": (o == sel_org and s == sel_sup)}
            for o, s in pairs
        ]

        users_data: List[Dict[str, Any]] = []
        roles_data: List[Dict[str, Any]] = []
        if sel_org and sel_sup:
            users_raw = [_normalize_user(u) for u in list_users(sel_org, sel_sup)]
            for u in users_raw:
                uname = str(u.get("username") or u.get("name") or "").strip()
                u["is_superuser"] = uname.lower() == sel_sup.lower()
            users_data = users_raw
            roles_data = list_roles(sel_org, sel_sup)

        ctx: Dict[str, Any] = {
            "request": request,
            "authorized": True,
            "token": provided,
            "tenants": tenants,
            "sel_org": sel_org,
            "sel_sup": sel_sup,
            "has_tenant": bool(sel_org and sel_sup),
            "users": users_data,
            "roles": roles_data,
        }
        inject_session_into_ctx(ctx, request)
        resp = templates.TemplateResponse("users.html", ctx)
        no_store(resp)
        return resp

    # ── API: list users (JSON, called after mutations to refresh table) ────────

    @router.get("/reflection/users-page/list")
    def api_list_users(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(admin_guard_api),
    ):
        o, s = _resolve_or_raise(org, sup)
        users_raw = [_normalize_user(u) for u in list_users(o, s)]
        # Mark the superuser so the frontend can hide destructive controls reliably.
        for u in users_raw:
            uname = str(u.get("username") or u.get("name") or "").strip()
            u["is_superuser"] = uname.lower() == s.lower()
        roles_data = list_roles(o, s)
        return {"ok": True, "users": users_raw, "roles": roles_data}

    # ── API: create user ──────────────────────────────────────────────────────

    @router.post("/reflection/users-page/create")
    def api_create_user(
        request: Request,
        payload: Dict[str, Any] = Body(...),
        _: Any = Depends(admin_guard_api),
    ):
        """
        Create a user and assign them a role.

        Body: { username, role_type, org, sup }
        role_type must be one of: superadmin, admin, writer, reader, meta

        Strategy:
          1. Validate inputs.
          2. Use RoleManager to find an existing role of the given type, or
             create a new default one (wildcard tables/columns/filters).
          3. Use UserManager to create the user with that role_id.
        """
        username = str(payload.get("username") or "").strip()
        if not username:
            raise HTTPException(status_code=400, detail="username is required")

        role_type = str(payload.get("role_type") or "").strip().lower()
        if role_type not in _VALID_ROLE_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"role_type must be one of: {', '.join(sorted(_VALID_ROLE_TYPES))}",
            )

        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup are required")

        # The superuser's username always equals the supertable name (sup / super_name).
        # It is provisioned out-of-band and must never be created from the UI.
        if username.lower() == sup.lower():
            raise HTTPException(
                status_code=403,
                detail="The superuser account cannot be created from the UI.",
            )

        # Belt-and-suspenders: also block the role_type=superadmin path entirely,
        # regardless of username — superadmin roles must not be assigned via the UI.
        if role_type == "superadmin":
            raise HTTPException(
                status_code=403,
                detail="The superadmin role cannot be assigned from the UI.",
            )

        rm = _role_manager(org, sup)
        um = _user_manager(org, sup)

        # Find or create a role of the requested type
        existing_roles = rm.get_roles_by_type(role_type)
        if existing_roles:
            role_id = existing_roles[0].get("role_id") or existing_roles[0].get("hash") or ""
        else:
            # Create a default role for this type
            role_id = rm.create_role({
                "role": role_type,
                "role_name": role_type,
                "tables": ["*"],
                "columns": ["*"],
                "filters": ["*"],
            })

        if not role_id:
            raise HTTPException(status_code=500, detail="Could not resolve role_id")

        try:
            user_id = um.create_user({"username": username, "roles": [role_id]})
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"User creation failed: {e}")

        return {"ok": True, "user_id": user_id, "username": username, "role_id": role_id}

    # ── API: delete user ──────────────────────────────────────────────────────

    @router.delete("/reflection/users-page/user/{user_id}")
    def api_delete_user(
        request: Request,
        user_id: str,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(admin_guard_api),
    ):
        o, s = _resolve_or_raise(org, sup)
        um = _user_manager(o, s)

        # Resolve the superuser's real user_id by scanning the user list.
        # The superuser's username always equals the supertable name (sup / super_name).
        # We intentionally use list_users (the same source the UI uses) so the lookup
        # is consistent with what was rendered — read_user cannot be relied upon here.
        try:
            all_users = list_users(o, s)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Could not verify user identity before deletion: {e}",
            )

        superuser_ids: set = {
            str(u.get("user_id") or u.get("hash") or "").strip()
            for u in all_users
            if str(u.get("username") or u.get("name") or "").strip().lower() == s.lower()
        }
        superuser_ids.discard("")

        if user_id in superuser_ids:
            raise HTTPException(
                status_code=403,
                detail="The superuser account cannot be deleted from the UI.",
            )

        try:
            um.delete_user(user_id)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Delete failed: {e}")
        return {"ok": True, "user_id": user_id}

    # ── API: add role to user ─────────────────────────────────────────────────

    @router.post("/reflection/users-page/user/{user_id}/role")
    def api_add_role(
        request: Request,
        user_id: str,
        payload: Dict[str, Any] = Body(...),
        _: Any = Depends(admin_guard_api),
    ):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        role_id = str(payload.get("role_id") or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup are required")
        if not role_id:
            raise HTTPException(status_code=400, detail="role_id is required")
        if not catalog.rbac_role_exists(org, sup, role_id):
            raise HTTPException(status_code=404, detail="Role not found")
        try:
            catalog.rbac_add_role_to_user(org, sup, user_id, role_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Add role failed: {e}")
        return {"ok": True, "user_id": user_id, "role_id": role_id}

    # ── API: remove role from user ────────────────────────────────────────────

    @router.delete("/reflection/users-page/user/{user_id}/role/{role_id}")
    def api_remove_role(
        request: Request,
        user_id: str,
        role_id: str,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(admin_guard_api),
    ):
        o, s = _resolve_or_raise(org, sup)
        try:
            catalog.rbac_remove_role_from_user(o, s, user_id, role_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Remove role failed: {e}")
        return {"ok": True, "user_id": user_id, "role_id": role_id}

    # ── API: user / role detail ───────────────────────────────────────────────

    @router.get("/reflection/user/{user_id}")
    def api_user_details(
        user_id: str,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(admin_guard_api),
    ):
        org_eff, sup_eff = resolve_pair(org, sup)
        if not org_eff or not sup_eff:
            raise HTTPException(status_code=404, detail="Tenant not found")
        obj = read_user(org_eff, sup_eff, user_id)
        if not obj:
            raise HTTPException(status_code=404, detail="User not found")
        return {"user_id": user_id, "data": obj}

    @router.get("/reflection/role/{role_id}")
    def api_role_details(
        role_id: str,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(admin_guard_api),
    ):
        org_eff, sup_eff = resolve_pair(org, sup)
        if not org_eff or not sup_eff:
            raise HTTPException(status_code=404, detail="Tenant not found")
        obj = read_role(org_eff, sup_eff, role_id)
        if not obj:
            raise HTTPException(status_code=404, detail="Role not found")
        return {"role_id": role_id, "data": obj}

    # ── API: tokens ───────────────────────────────────────────────────────────

    @router.get("/reflection/tokens")
    def api_list_tokens(
        request: Request,
        org: Optional[str] = Query(None),
        _: Any = Depends(admin_guard_api),
    ):
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        org_eff = (org or _get_org_from_env_fallback()).strip()
        return {"ok": True, "organization": org_eff, "tokens": _catalog_list_tokens(org_eff)}

    @router.post("/reflection/tokens")
    def api_create_token(
        request: Request,
        org: Optional[str] = Query(None),
        label: str = Query(""),
        _: Any = Depends(admin_guard_api),
    ):
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        org_eff = (org or _get_org_from_env_fallback()).strip()
        created = _catalog_create_token(org_eff, created_by="superuser", label=label)
        return {"ok": True, "organization": org_eff, **created}

    @router.delete("/reflection/tokens/{token_id}")
    def api_delete_token(
        request: Request,
        token_id: str,
        org: Optional[str] = Query(None),
        _: Any = Depends(admin_guard_api),
    ):
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        org_eff = (org or _get_org_from_env_fallback()).strip()
        if not _catalog_delete_token(org_eff, token_id):
            raise HTTPException(status_code=404, detail="Token not found")
        return {"ok": True, "organization": org_eff, "token_id": token_id}
