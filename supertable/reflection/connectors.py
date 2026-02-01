from __future__ import annotations

import base64
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Sequence, Tuple
from urllib.parse import urlparse

import requests
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

try:
    from cryptography.fernet import Fernet, InvalidToken
except Exception:  # pragma: no cover
    Fernet = None  # type: ignore
    InvalidToken = Exception  # type: ignore

try:
    from .redis_connector import RedisConnector
except Exception:  # pragma: no cover
    try:
        from redis_connector import RedisConnector  # type: ignore
    except Exception:
        RedisConnector = None  # type: ignore


# ---------------------------------------------------------------------------
# Persistence (simple JSON on disk)
# ---------------------------------------------------------------------------

def _state_dir() -> Path:
    base = Path(os.getenv("SUPERTABLE_REFLECTION_STATE_DIR", "/tmp/supertable_reflection"))
    d = base / "connectors"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _safe_slug(v: str) -> str:
    v = (v or "").strip()
    out = []
    for ch in v:
        if ch.isalnum() or ch in ("-", "_", "."):
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)[:200] or "x"


def _tenant_path(org: str, sup: str) -> Path:
    return _state_dir() / f"{_safe_slug(org)}__{_safe_slug(sup)}.json"


def _load_saved(org: str, sup: str) -> Dict[str, Any]:
    p = _tenant_path(org, sup)
    if not p.exists():
        return {"items": []}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"items": []}


def _save_saved(org: str, sup: str, data: Dict[str, Any]) -> None:
    p = _tenant_path(org, sup)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Vault (encrypted secrets in Redis, scoped per SuperTable)
# ---------------------------------------------------------------------------

_redis_singleton: Any = None
_fernet_singleton: Any = None


def _b2s(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8")
        except Exception:
            return v.decode("latin-1", errors="ignore")
    return str(v)


def _get_redis():
    global _redis_singleton
    if _redis_singleton is not None:
        return _redis_singleton
    if RedisConnector is None:
        raise RuntimeError("RedisConnector not available")
    _redis_singleton = RedisConnector().r
    return _redis_singleton


def _derive_fernet_key_from_master(master: str) -> bytes:
    # Fernet expects a urlsafe-base64 32-byte key.
    digest = hashlib.sha256((master or "").encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


def _get_fernet():
    global _fernet_singleton
    if _fernet_singleton is not None:
        return _fernet_singleton
    if Fernet is None:
        raise RuntimeError("cryptography.fernet not available")

    env_key = (os.getenv("SUPERTABLE_VAULT_FERNET_KEY") or "").strip()
    if env_key:
        _fernet_singleton = Fernet(env_key.encode("utf-8"))
        return _fernet_singleton

    master = (os.getenv("SUPERTABLE_VAULT_MASTER_KEY") or "").strip()
    if not master:
        # Best-effort fallbacks if your app already has a stable master secret
        for k in ("SUPERTABLE_SECRET_KEY", "SUPERTABLE_AUTH_SECRET", "SUPERTABLE_JWT_SECRET"):
            v = (os.getenv(k) or "").strip()
            if v:
                master = v
                break

    if not master:
        raise RuntimeError(
            "Vault encryption key missing. Set SUPERTABLE_VAULT_FERNET_KEY (preferred) "
            "or SUPERTABLE_VAULT_MASTER_KEY."
        )

    _fernet_singleton = Fernet(_derive_fernet_key_from_master(master))
    return _fernet_singleton


def _vault_meta_key(org: str, sup: str) -> str:
    # Set of secret names for fast listing
    return f"supertable:{org}:{sup}:meta:vault:meta"


def _vault_item_key(org: str, sup: str, name: str) -> str:
    # Individual secret entry (JSON) per name
    return f"supertable:{org}:{sup}:meta:vault:{_safe_slug(name)}"


def _vault_encrypt(plaintext: str) -> str:
    f = _get_fernet()
    token = f.encrypt((plaintext or "").encode("utf-8"))
    return _b2s(token)


def _vault_decrypt(token: str) -> str:
    f = _get_fernet()
    try:
        out = f.decrypt((token or "").encode("utf-8"))
    except InvalidToken as e:
        raise HTTPException(status_code=400, detail="Invalid vault ciphertext") from e
    return _b2s(out)


# ---------------------------------------------------------------------------
# Airbyte API helpers
# ---------------------------------------------------------------------------

def _validate_base_url(base_url: str) -> str:
    base_url = (base_url or "").strip()
    if not base_url:
        raise HTTPException(status_code=400, detail="base_url is required")
    u = urlparse(base_url)
    if u.scheme not in ("http", "https"):
        raise HTTPException(status_code=400, detail="base_url must be http(s)")
    return base_url.rstrip("/")


def _airbyte_post(base_url: str, path: str, payload: Dict[str, Any]) -> Any:
    base_url = _validate_base_url(base_url)
    url = f"{base_url}{path}"
    try:
        r = requests.post(url, json=payload, timeout=25)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        detail = ""
        try:
            detail = r.text
        except Exception:
            detail = str(e)
        raise HTTPException(status_code=502, detail=f"Airbyte error: {detail}") from e
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Airbyte request failed: {e}") from e


def attach_connectors_routes(
        router: APIRouter,
        *,
        templates: Any,
        is_authorized: Callable[[Request], bool],
        no_store: Callable[[Any], None],
        get_provided_token: Callable[[Request], Optional[str]],
        discover_pairs: Callable[[], Sequence[Tuple[str, str]]],
        resolve_pair: Callable[[Optional[str], Optional[str]], Tuple[Optional[str], Optional[str]]],
        inject_session_into_ctx: Callable[[Dict[str, Any], Request], None],
        logged_in_guard_api: Any = None,
        admin_guard_api: Any = None,
) -> None:
    # Backward-compatible guard defaults if caller doesn't pass the dependencies.
    def _fallback_logged_in_guard(request: Request) -> bool:
        if not is_authorized(request):
            raise HTTPException(status_code=401, detail="Not authorized")
        return True

    _logged_dep = logged_in_guard_api or _fallback_logged_in_guard
    _admin_dep = admin_guard_api or _fallback_logged_in_guard

    @router.get("/reflection/connectors", response_class=HTMLResponse)
    def connectors_page(
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
        tenants = [{"org": o, "sup": s, "selected": (o == sel_org and s == sel_sup)} for o, s in pairs]

        ctx: Dict[str, Any] = {
            "request": request,
            "authorized": True,
            "token": provided,
            "tenants": tenants,
            "sel_org": sel_org,
            "sel_sup": sel_sup,
            "has_tenant": bool(sel_org and sel_sup),
        }
        inject_session_into_ctx(ctx, request)
        resp = templates.TemplateResponse("connectors.html", ctx)
        no_store(resp)
        return resp

    # --- Airbyte discovery endpoints ---

    @router.post("/reflection/connectors/airbyte/workspaces/list")
    def airbyte_list_workspaces(payload: Dict[str, Any] = Body(...), _: Any = Depends(_logged_dep)):
        base_url = str(payload.get("base_url") or "")
        return _airbyte_post(base_url, "/api/v1/workspaces/list", {})

    @router.post("/reflection/connectors/airbyte/source_definitions/list")
    def airbyte_list_source_defs(payload: Dict[str, Any] = Body(...), _: Any = Depends(_logged_dep)):
        base_url = str(payload.get("base_url") or "")
        return _airbyte_post(base_url, "/api/v1/source_definitions/list", {})

    @router.post("/reflection/connectors/airbyte/destination_definitions/list")
    def airbyte_list_dest_defs(payload: Dict[str, Any] = Body(...), _: Any = Depends(_logged_dep)):
        base_url = str(payload.get("base_url") or "")
        return _airbyte_post(base_url, "/api/v1/destination_definitions/list", {})

    @router.post("/reflection/connectors/airbyte/sources/create")
    def airbyte_create_source(payload: Dict[str, Any] = Body(...), _: Any = Depends(_logged_dep)):
        base_url = str(payload.get("base_url") or "")
        body = {
            "name": payload.get("name"),
            "workspaceId": payload.get("workspaceId"),
            "sourceDefinitionId": payload.get("sourceDefinitionId"),
            "connectionConfiguration": payload.get("connectionConfiguration") or {},
        }
        return _airbyte_post(base_url, "/api/v1/sources/create", body)

    @router.post("/reflection/connectors/airbyte/destinations/create")
    def airbyte_create_destination(payload: Dict[str, Any] = Body(...), _: Any = Depends(_logged_dep)):
        base_url = str(payload.get("base_url") or "")
        body = {
            "name": payload.get("name"),
            "workspaceId": payload.get("workspaceId"),
            "destinationDefinitionId": payload.get("destinationDefinitionId"),
            "connectionConfiguration": payload.get("connectionConfiguration") or {},
        }
        return _airbyte_post(base_url, "/api/v1/destinations/create", body)

    # --- Saved connectors ---

    @router.get("/reflection/connectors/saved")
    def saved_list(org: str, sup: str, _: Any = Depends(_logged_dep)):
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")
        return {"ok": True, "data": _load_saved(org, sup)}

    @router.post("/reflection/connectors/saved/upsert")
    def saved_upsert(payload: Dict[str, Any] = Body(...), _: Any = Depends(_logged_dep)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        item = payload.get("item") or {}
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org/sup required")

        data = _load_saved(org, sup)
        items = data.get("items") or []
        item_id = str(item.get("id") or "").strip() or os.urandom(8).hex()
        item["id"] = item_id

        out = []
        replaced = False
        for it in items:
            if isinstance(it, dict) and str(it.get("id")) == item_id:
                out.append(item)
                replaced = True
            else:
                out.append(it)
        if not replaced:
            out.insert(0, item)
        data["items"] = out
        _save_saved(org, sup, data)
        return {"ok": True, "id": item_id}

    @router.delete("/reflection/connectors/saved/{item_id}")
    def saved_delete(item_id: str, org: str, sup: str, _: Any = Depends(_logged_dep)):
        data = _load_saved(org, sup)
        items = data.get("items") or []
        data["items"] = [it for it in items if not (isinstance(it, dict) and str(it.get("id")) == item_id)]
        _save_saved(org, sup, data)
        return {"ok": True}

    # --- Vault (encrypted secrets in Redis per SuperTable) ---

    @router.get("/reflection/connectors/vault/list")
    def vault_list(org: str, sup: str, _: Any = Depends(_logged_dep)):
        org = str(org or "").strip()
        sup = str(sup or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        try:
            r = _get_redis()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

        meta_key = _vault_meta_key(org, sup)
        names = list(r.smembers(meta_key) or [])
        names = [_b2s(n) for n in names if _b2s(n)]
        names.sort()

        items: list[Dict[str, Any]] = []
        if names:
            pipe = r.pipeline()
            for n in names:
                pipe.get(_vault_item_key(org, sup, n))
            blobs = pipe.execute()

            for n, blob in zip(names, blobs):
                if not blob:
                    continue
                try:
                    js = json.loads(_b2s(blob))
                except Exception:
                    js = {"name": n}
                # never expose ciphertext in list
                js.pop("ciphertext", None)
                js["name"] = js.get("name") or n
                js["redis_key"] = js.get("redis_key") or _vault_item_key(org, sup, n)
                js["meta_key"] = js.get("meta_key") or meta_key
                js["has_value"] = True
                items.append(js)

        return {"ok": True, "meta_key": meta_key, "items": items}

    @router.post("/reflection/connectors/vault/upsert")
    def vault_upsert(payload: Dict[str, Any] = Body(...), _: Any = Depends(_admin_dep)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        name = str(payload.get("name") or "").strip()
        value = str(payload.get("value") or "")
        note = str(payload.get("note") or "").strip()
        user_hash = str(payload.get("user_hash") or "").strip() or None

        if not org or not sup:
            raise HTTPException(status_code=400, detail="org/sup required")
        if not name:
            raise HTTPException(status_code=400, detail="name required")
        if value == "":
            raise HTTPException(status_code=400, detail="value required")

        try:
            r = _get_redis()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

        meta_key = _vault_meta_key(org, sup)
        item_key = _vault_item_key(org, sup, name)

        now_ms = int(time.time() * 1000)

        created_ms = now_ms
        created_by = user_hash

        existing = r.get(item_key)
        if existing:
            try:
                old = json.loads(_b2s(existing))
                created_ms = int(old.get("created_at_ms") or created_ms)
                created_by = old.get("created_by") or created_by
            except Exception:
                pass

        try:
            ciphertext = _vault_encrypt(value)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

        entry = {
            "name": name,
            "note": note,
            "enc": "fernet",
            "ciphertext": ciphertext,
            "created_at_ms": created_ms,
            "updated_at_ms": now_ms,
            "created_by": created_by,
            "updated_by": user_hash,
            "redis_key": item_key,
            "meta_key": meta_key,
        }

        pipe = r.pipeline()
        pipe.set(item_key, json.dumps(entry, ensure_ascii=False))
        pipe.sadd(meta_key, name)
        pipe.execute()

        return {"ok": True, "name": name, "redis_key": item_key, "meta_key": meta_key}

    @router.post("/reflection/connectors/vault/reveal")
    def vault_reveal(payload: Dict[str, Any] = Body(...), _: Any = Depends(_admin_dep)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        name = str(payload.get("name") or "").strip()
        if not org or not sup or not name:
            raise HTTPException(status_code=400, detail="org/sup/name required")

        try:
            r = _get_redis()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

        item_key = _vault_item_key(org, sup, name)
        blob = r.get(item_key)
        if not blob:
            raise HTTPException(status_code=404, detail="secret not found")

        try:
            js = json.loads(_b2s(blob))
        except Exception:
            raise HTTPException(status_code=500, detail="vault entry corrupt")

        ciphertext = str(js.get("ciphertext") or "")
        if not ciphertext:
            raise HTTPException(status_code=500, detail="vault entry missing ciphertext")

        try:
            value = _vault_decrypt(ciphertext)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

        return {"ok": True, "name": name, "value": value, "redis_key": item_key}

    @router.delete("/reflection/connectors/vault/{name}")
    def vault_delete(name: str, org: str, sup: str, _: Any = Depends(_admin_dep)):
        org = str(org or "").strip()
        sup = str(sup or "").strip()
        name = str(name or "").strip()
        if not org or not sup or not name:
            raise HTTPException(status_code=400, detail="org/sup/name required")

        try:
            r = _get_redis()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

        meta_key = _vault_meta_key(org, sup)
        item_key = _vault_item_key(org, sup, name)

        pipe = r.pipeline()
        pipe.delete(item_key)
        pipe.srem(meta_key, name)
        pipe.execute()
        return {"ok": True}

    # --- PyAirbyte Utils ---

    def _import_pyairbyte_like() -> Tuple[Optional[str], Optional[Any]]:
        try:
            import airbyte as ab
            return "airbyte", ab
        except Exception:
            try:
                import pyairbyte as ab
                return "pyairbyte", ab
            except Exception:
                return None, None

    def _popular_connectors(all_names: Sequence[str]) -> Dict[str, list[str]]:
        sources = ["source-postgres", "source-mysql", "source-s3", "source-google-sheets", "source-github"]
        dests = ["destination-postgres", "destination-s3", "destination-bigquery"]
        sset = set(all_names)
        return {
            "sources": [n for n in sources if n in sset],
            "destinations": [n for n in dests if n in sset],
        }

    @router.get("/reflection/connectors/pyairbyte/connectors")
    def pyairbyte_connectors(_: Any = Depends(_logged_dep)):
        modname, ab = _import_pyairbyte_like()
        if ab is None:
            return {"ok": True, "installed": False, "connectors": []}

        names = []
        try:
            # Current PyAirbyte API
            names = list(ab.get_available_connectors())
        except Exception:
            try:
                # Legacy or variant API
                names = list(ab.get_available_sources()) + list(ab.get_available_destinations())
            except Exception:
                pass

        return {
            "ok": True,
            "installed": True,
            "module": modname,
            "connectors": sorted(list(set(names))),
            "popular": _popular_connectors(names)
        }

    @router.post("/reflection/connectors/pyairbyte/spec")
    def pyairbyte_spec(payload: Dict[str, Any] = Body(...), _: Any = Depends(_logged_dep)):
        modname, ab = _import_pyairbyte_like()
        kind = str(payload.get("kind") or "source")
        connector = str(payload.get("connector") or "")

        if ab is None:
            return {"ok": False, "error": "pyairbyte not installed"}

        factory = getattr(ab, "get_source" if kind == "source" else "get_destination", None)
        if not factory:
            raise HTTPException(status_code=400, detail="API not found")

        try:
            obj = factory(connector, config={})
            spec = getattr(obj, "config_spec", {})
            if callable(spec):
                spec = spec()
            return {"ok": True, "spec": spec, "module": modname}
        except Exception as e:
            return {"ok": False, "error": str(e), "module": modname}

    @router.post("/reflection/connectors/pyairbyte/check")
    def pyairbyte_check(payload: Dict[str, Any] = Body(...), _: Any = Depends(_logged_dep)):
        _, ab = _import_pyairbyte_like()
        if ab is None:
            return {"ok": False, "error": "pyairbyte not installed"}

        kind = payload.get("kind")
        connector = payload.get("connector")
        config = payload.get("config") or {}

        factory = getattr(ab, "get_source" if kind == "source" else "get_destination")
        try:
            obj = factory(connector, config=config)
            res = obj.check()
            return {"ok": True, "status": str(res)}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @router.post("/reflection/connectors/pyairbyte/codegen")
    def pyairbyte_codegen(payload: Dict[str, Any] = Body(...), _: Any = Depends(_logged_dep)):
        kind = payload.get("kind")
        connector = payload.get("connector")
        config = payload.get("config") or {}
        name = _safe_slug(payload.get("name") or "conn").replace("-", "_")

        cfg_str = json.dumps(config, indent=2)
        method = "get_source" if kind == "source" else "get_destination"

        code = (
            "import airbyte as ab\n\n"
            f"config = {cfg_str}\n\n"
            f"{name} = ab.{method}(\n"
            f"    {connector!r},\n"
            "    config=config,\n"
            "    install_if_missing=True\n"
            ")\n\n"
            f"verify = {name}.check()\n"
            "print(verify)"
        )
        return {"ok": True, "code": code}

    @router.get("/reflection/connectors/pyairbyte/status")
    def pyairbyte_status(_: Any = Depends(_logged_dep)):
        modname, ab = _import_pyairbyte_like()
        return {"ok": True, "installed": ab is not None, "module": modname}
