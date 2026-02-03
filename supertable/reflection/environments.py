from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import os
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Sequence, Tuple
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
    d = base / "environments"
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
# Environments (.env-style encrypted blobs)
# ---------------------------------------------------------------------------

_DOTENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _default_env_stages() -> list[str]:
    raw = (os.getenv("SUPERTABLE_DEFAULT_STAGES") or "").strip()
    if not raw:
        raw = "dev,test,uat,stage,prod"
    out: list[str] = []
    for part in raw.split(","):
        s = (part or "").strip()
        if not s:
            continue
        s = _safe_slug(s)
        if s and s not in out:
            out.append(s)
    return out or ["dev"]


def _envs_stages_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:envs:stages"


def _envs_meta_key(org: str, sup: str) -> str:
    # Set of environment names
    return f"supertable:{org}:{sup}:meta:envs:meta"


def _envs_env_meta_key(org: str, sup: str, name: str) -> str:
    return f"supertable:{org}:{sup}:meta:envs:{_safe_slug(name)}:meta"


def _envs_item_key(org: str, sup: str, name: str, stage: str) -> str:
    return f"supertable:{org}:{sup}:meta:envs:{_safe_slug(name)}:{_safe_slug(stage)}"


def _validate_stage_name(stage: str) -> str:
    stage = str(stage or "").strip()
    if not stage:
        raise HTTPException(status_code=400, detail="stage is required")
    stage = _safe_slug(stage)
    if not stage:
        raise HTTPException(status_code=400, detail="invalid stage")
    return stage


def _validate_env_name(name: str) -> str:
    name = str(name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    # We keep the original name for display, but ensure it has a stable slug for storage.
    if not _safe_slug(name):
        raise HTTPException(status_code=400, detail="invalid name")
    return name


def _validate_dotenv(content: str) -> None:
    # Lenient .env validation: allow comments and blank lines; require KEY=VALUE otherwise.
    errors: list[str] = []
    lines = (content or "").splitlines()
    for i, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            errors.append(f"line {i}: expected KEY=VALUE")
            continue
        key, _ = line.split("=", 1)
        key = key.strip()
        if not key or not _DOTENV_KEY_RE.match(key):
            errors.append(f"line {i}: invalid key {key!r}")
            continue
    if errors:
        raise HTTPException(status_code=400, detail="Invalid .env format: " + "; ".join(errors[:10]))


def _dotenv_parse_vars(content: str) -> Dict[str, str]:
    """Parse a .env formatted string into a dict.

    This is intentionally lenient and mirrors common .env behavior:
    - blank lines / comments are ignored
    - supports optional 'export ' prefix
    - KEY=VALUE with VALUE optionally quoted
    """
    out: Dict[str, str] = {}
    lines = (content or "").splitlines()
    for raw in lines:
        line = (raw or "").strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        if not key:
            continue

        v = val.strip()

        # If unquoted, strip inline comments that follow whitespace.
        if v and v[0] not in ("'", '"'):
            v = re.split(r"\s+#", v, maxsplit=1)[0].rstrip()

        # Handle single or double quoted values
        if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
            q = v[0]
            inner = v[1:-1]
            if q == '"':
                # basic escape sequences
                inner = (
                    inner.replace('\\n', '\n')
                    .replace('\\r', '\r')
                    .replace('\\t', '\t')
                    .replace('\\"', '"')
                    .replace('\\\\', '\\')
                )
            else:
                inner = inner.replace("\\'", "'").replace('\\\\', '\\')
            out[key] = inner
        else:
            out[key] = v
    return out


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_decode(val: str) -> bytes:
    v = (val or "").strip()
    pad = "=" * ((4 - (len(v) % 4)) % 4)
    return base64.urlsafe_b64decode((v + pad).encode("ascii"))


def _env_export_secret() -> bytes:
    raw = (os.getenv("SUPERTABLE_ENV_EXPORT_SECRET") or os.getenv("SUPERTABLE_SESSION_SECRET") or "").strip()
    if raw:
        return raw.encode("utf-8")

    # Best-effort fallbacks if your app already has a stable master secret
    for k in ("SUPERTABLE_VAULT_MASTER_KEY", "SUPERTABLE_SECRET_KEY", "SUPERTABLE_AUTH_SECRET", "SUPERTABLE_JWT_SECRET"):
        v = (os.getenv(k) or "").strip()
        if v:
            return v.encode("utf-8")

    raise RuntimeError(
        "Missing env export signing secret. Set SUPERTABLE_ENV_EXPORT_SECRET "
        "(preferred) or SUPERTABLE_SESSION_SECRET."
    )


def _env_export_token_encode(data: Dict[str, Any]) -> str:
    payload = json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    sig = hmac.new(_env_export_secret(), payload, hashlib.sha256).digest()
    return _b64url_encode(payload) + "." + _b64url_encode(sig)


def _env_export_token_decode(token: str) -> Optional[Dict[str, Any]]:
    try:
        if not token or "." not in token:
            return None
        b64_payload, b64_sig = token.split(".", 1)
        payload = _b64url_decode(b64_payload)
        sig = _b64url_decode(b64_sig)
        expected = hmac.new(_env_export_secret(), payload, hashlib.sha256).digest()
        if not hmac.compare_digest(expected, sig):
            return None
        data = json.loads(payload.decode("utf-8"))
        if not isinstance(data, dict):
            return None
        exp = data.get("exp")
        if exp is not None:
            try:
                if int(exp) < int(time.time()):
                    return None
            except Exception:
                return None
        return data
    except Exception:
        return None


def _saved_source_meta_key(org: str, sup: str, kind: str) -> str:
    k = "destination" if str(kind or "").strip().lower() == "destination" else "source"
    return f"supertable:{org}:{sup}:meta:saved:{k}:meta"


def _saved_source_item_key(org: str, sup: str, kind: str, item_id: str) -> str:
    k = "destination" if str(kind or "").strip().lower() == "destination" else "source"
    return f"supertable:{org}:{sup}:meta:saved:{k}:{_safe_slug(item_id)}"

def attach_environments_routes(
        router: APIRouter,
        *,
        templates: Any,
        is_authorized: Callable[[Request], bool],
        no_store: Callable[[Any], None],
        get_provided_token: Callable[[Request], Optional[str]],
        discover_pairs: Callable[[], Sequence[Tuple[str, str]]],
        resolve_pair: Callable[[Optional[str], Optional[str]], Tuple[Optional[str], Optional[str]]],
        inject_session_into_ctx: Callable[[Dict[str, Any], Request], Dict[str, Any]],
        logged_in_guard_api: Callable[[Request], Any],
        admin_guard_api: Callable[[Request], Any],
) -> None:
    """Attach Environments UI + API routes."""

    def _logged_dep(request: Request):
        return logged_in_guard_api(request)

    def _admin_dep(request: Request):
        return admin_guard_api(request)

    @router.get("/reflection/environments", response_class=HTMLResponse)
    def environments_page(
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
        resp = templates.TemplateResponse("environments.html", ctx)
        no_store(resp)
        return resp

    def _envs_get_stages(org: str, sup: str) -> list[str]:
        r = _get_redis()
        key = _envs_stages_key(org, sup)
        raw = r.get(key)
        if raw:
            try:
                js = json.loads(_b2s(raw))
                if isinstance(js, list):
                    out = []
                    for it in js:
                        s = _safe_slug(str(it or "").strip())
                        if s and s not in out:
                            out.append(s)
                    if out:
                        return out
            except Exception:
                pass
        return _default_env_stages()

    # stages
    @router.get("/reflection/environments/envs/stages")
    def envs_stages(org: str, sup: str, _: Any = Depends(_logged_dep)):
        org = str(org or "").strip()
        sup = str(sup or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")
        try:
            stages = _envs_get_stages(org, sup)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e
        return {"ok": True, "stages": stages}

    @router.post("/reflection/environments/envs/stages/add")
    def envs_stage_add(payload: Dict[str, Any] = Body(...), _: Any = Depends(_admin_dep)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        stage = _validate_stage_name(payload.get("stage"))
        user_hash = str(payload.get("user_hash") or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        r = _get_redis()
        stages = _envs_get_stages(org, sup)
        if stage in stages:
            return {"ok": True, "stages": stages}

        stages.append(stage)
        r.set(_envs_stages_key(org, sup), json.dumps(stages))
        return {"ok": True, "stages": stages, "updated_by": user_hash, "updated_at": time.time()}

    @router.post("/reflection/environments/envs/stages/delete")
    def envs_stage_delete(payload: Dict[str, Any] = Body(...), _: Any = Depends(_admin_dep)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        stage = _validate_stage_name(payload.get("stage"))
        user_hash = str(payload.get("user_hash") or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        r = _get_redis()
        stages = _envs_get_stages(org, sup)
        if stage not in stages:
            return {"ok": True, "stages": stages}
        if len(stages) <= 1:
            raise HTTPException(status_code=400, detail="cannot delete the last stage")

        stages = [s for s in stages if s != stage]
        r.set(_envs_stages_key(org, sup), json.dumps(stages))

        # Remove per-stage values for all environments
        env_names = [_b2s(n) for n in (r.smembers(_envs_meta_key(org, sup)) or [])]
        for name in env_names:
            if not name:
                continue
            r.delete(_envs_item_key(org, sup, name, stage))

        return {"ok": True, "stages": stages, "updated_by": user_hash, "updated_at": time.time()}

    # list
    @router.get("/reflection/environments/envs/list")
    def envs_list(org: str, sup: str, _: Any = Depends(_logged_dep)):
        org = str(org or "").strip()
        sup = str(sup or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        r = _get_redis()
        stages = _envs_get_stages(org, sup)
        names = [_b2s(n) for n in (r.smembers(_envs_meta_key(org, sup)) or [])]
        names = [n for n in names if n]
        names.sort()

        items: list[Dict[str, Any]] = []
        for name in names:
            stage_meta: Dict[str, Any] = {}
            last_at = 0.0
            last_by = ""
            for st in stages:
                blob = r.get(_envs_item_key(org, sup, name, st))
                if not blob:
                    continue
                try:
                    js = json.loads(_b2s(blob))
                except Exception:
                    continue
                js.pop("ciphertext", None)
                at = float(js.get("updated_at") or 0.0)
                by = str(js.get("updated_by") or "")
                stage_meta[st] = {"updated_at": at, "updated_by": by, "note": js.get("note") or ""}
                if at >= last_at:
                    last_at = at
                    last_by = by
            items.append(
                {
                    "name": name,
                    "last_updated_at": last_at,
                    "last_updated_by": last_by,
                    "stages": stage_meta,
                }
            )

        items.sort(key=lambda x: float(x.get("last_updated_at") or 0.0), reverse=True)
        return {"ok": True, "stages": stages, "items": items}

    # get
    @router.post("/reflection/environments/envs/get")
    def envs_get(payload: Dict[str, Any] = Body(...), _: Any = Depends(_logged_dep)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        name = _validate_env_name(payload.get("name"))
        stage = _validate_stage_name(payload.get("stage"))
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        r = _get_redis()
        blob = r.get(_envs_item_key(org, sup, name, stage))
        if not blob:
            return {"ok": True, "name": name, "stage": stage, "content": "", "found": False}

        try:
            js = json.loads(_b2s(blob))
        except Exception:
            raise HTTPException(status_code=500, detail="corrupt environment entry")

        ciphertext = str(js.get("ciphertext") or "")
        if not ciphertext:
            return {"ok": True, "name": name, "stage": stage, "content": "", "found": False}

        content = _vault_decrypt(ciphertext)
        return {
            "ok": True,
            "name": name,
            "stage": stage,
            "content": content,
            "found": True,
            "updated_at": js.get("updated_at"),
            "updated_by": js.get("updated_by"),
            "note": js.get("note") or "",
        }

    # export URL (signed) for notebooks etc.
    @router.post("/reflection/environments/envs/export_url")
    def envs_export_url(payload: Dict[str, Any] = Body(...), _: Any = Depends(_logged_dep)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        name = _validate_env_name(payload.get("name"))
        stage = _validate_stage_name(payload.get("stage"))
        ttl = int(payload.get("ttl_seconds") or 365 * 24 * 3600)
        ttl = max(60, min(ttl, 10 * 365 * 24 * 3600))
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        exp = int(time.time()) + ttl
        token = _env_export_token_encode(
            {
                "v": 1,
                "org": org,
                "sup": sup,
                "name": name,
                "stage": stage,
                "iat": int(time.time()),
                "exp": exp,
            }
        )
        rel = f"/reflection/environments/envs/export?token={token}"
        return {"ok": True, "relative_url": rel, "token": token, "exp": exp}

    @router.get("/reflection/environments/envs/export")
    def envs_export(token: str):
        data = _env_export_token_decode(token)
        if not data:
            raise HTTPException(status_code=401, detail="invalid token")

        org = str(data.get("org") or "").strip()
        sup = str(data.get("sup") or "").strip()
        name = _validate_env_name(data.get("name"))
        stage = _validate_stage_name(data.get("stage"))
        if not org or not sup:
            raise HTTPException(status_code=401, detail="invalid token scope")

        r = _get_redis()
        blob = r.get(_envs_item_key(org, sup, name, stage))
        if not blob:
            return {"ok": True, "name": name, "stage": stage, "vars": {}, "items": []}

        try:
            js = json.loads(_b2s(blob))
        except Exception:
            raise HTTPException(status_code=500, detail="corrupt environment entry")

        ciphertext = str(js.get("ciphertext") or "")
        if not ciphertext:
            return {"ok": True, "name": name, "stage": stage, "vars": {}, "items": []}

        content = _vault_decrypt(ciphertext)
        vars_dict = _dotenv_parse_vars(content)
        items = [{"key": k, "value": v} for k, v in vars_dict.items()]
        return {"ok": True, "name": name, "stage": stage, "vars": vars_dict, "items": items}

    # create / update
    @router.post("/reflection/environments/envs/create")
    def envs_create(payload: Dict[str, Any] = Body(...), _: Any = Depends(_admin_dep)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        name = _validate_env_name(payload.get("name"))
        stage = _validate_stage_name(payload.get("stage"))
        content = str(payload.get("content") or "")
        note = str(payload.get("note") or "")
        user_hash = str(payload.get("user_hash") or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        _validate_dotenv(content)

        r = _get_redis()
        stages = _envs_get_stages(org, sup)
        if stage not in stages:
            raise HTTPException(status_code=400, detail="unknown stage")

        now = time.time()
        ciphertext = _vault_encrypt(content)
        item = {
            "name": name,
            "stage": stage,
            "ciphertext": ciphertext,
            "updated_at": now,
            "updated_by": user_hash,
            "note": note,
        }
        r.set(_envs_item_key(org, sup, name, stage), json.dumps(item, ensure_ascii=False))
        r.sadd(_envs_meta_key(org, sup), name)

        meta_key = _envs_env_meta_key(org, sup, name)
        if not r.get(meta_key):
            r.set(meta_key, json.dumps({"created_at": now, "created_by": user_hash}, ensure_ascii=False))

        return {"ok": True, "name": name, "stage": stage, "updated_at": now, "updated_by": user_hash}

    @router.post("/reflection/environments/envs/update")
    def envs_update(payload: Dict[str, Any] = Body(...), _: Any = Depends(_admin_dep)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        name = _validate_env_name(payload.get("name"))
        stage = _validate_stage_name(payload.get("stage"))
        content = str(payload.get("content") or "")
        note = str(payload.get("note") or "")
        user_hash = str(payload.get("user_hash") or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        _validate_dotenv(content)

        r = _get_redis()
        if name not in [_b2s(n) for n in (r.smembers(_envs_meta_key(org, sup)) or [])]:
            raise HTTPException(status_code=404, detail="environment not found")

        stages = _envs_get_stages(org, sup)
        if stage not in stages:
            raise HTTPException(status_code=400, detail="unknown stage")

        now = time.time()
        ciphertext = _vault_encrypt(content)
        item = {
            "name": name,
            "stage": stage,
            "ciphertext": ciphertext,
            "updated_at": now,
            "updated_by": user_hash,
            "note": note,
        }
        r.set(_envs_item_key(org, sup, name, stage), json.dumps(item, ensure_ascii=False))
        return {"ok": True, "name": name, "stage": stage, "updated_at": now, "updated_by": user_hash}

    # delete
    @router.delete("/reflection/environments/envs/{name}")
    def envs_delete(name: str, org: str, sup: str, stage: Optional[str] = Query(None), _: Any = Depends(_admin_dep)):
        org = str(org or "").strip()
        sup = str(sup or "").strip()
        name = _validate_env_name(name)
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        r = _get_redis()
        stages = _envs_get_stages(org, sup)

        stage_q = str(stage or "").strip() or None
        if stage_q:
            stage_name = _validate_stage_name(stage_q)
            if stage_name not in stages:
                raise HTTPException(status_code=400, detail="unknown stage")

            r.delete(_envs_item_key(org, sup, name, stage_name))

            has_any = False
            for st in stages:
                if r.exists(_envs_item_key(org, sup, name, st)):
                    has_any = True
                    break

            if not has_any:
                r.delete(_envs_env_meta_key(org, sup, name))
                r.srem(_envs_meta_key(org, sup), name)

            return {"ok": True, "deleted_stage": stage_name, "remaining": has_any}

        for st in stages:
            r.delete(_envs_item_key(org, sup, name, st))
        r.delete(_envs_env_meta_key(org, sup, name))
        r.srem(_envs_meta_key(org, sup), name)
        return {"ok": True}
