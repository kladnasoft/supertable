from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import secrets
import time
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


# =============================================================================
# Redis + helpers
# =============================================================================

_redis_singleton: Any = None
_fernet_singleton: Any = None

_DOTENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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
    """
    Use the same Redis connection used across reflection services.

    This relies on RedisConnector which supports:
      - Sentinel
      - direct Redis URL fallback
      - SUPERTABLE_REDIS_DB / passwords
    """
    global _redis_singleton
    if _redis_singleton is not None:
        return _redis_singleton
    if RedisConnector is None:
        raise RuntimeError("RedisConnector not available")
    _redis_singleton = RedisConnector().r
    return _redis_singleton


def _safe_slug(v: str) -> str:
    v = (v or "").strip()
    out = []
    for ch in v:
        if ch.isalnum() or ch in ("-", "_", "."):
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)[:200] or "x"



def _is_hex_hash(v: str) -> bool:
    v = (v or "").strip()
    return bool(re.fullmatch(r"[0-9a-f]{32,64}", v))


def _actor_from_payload(payload: Dict[str, Any]) -> Tuple[str, str]:
    """Return (actor_name, actor_hash). Prefer human username for display."""
    name = str(payload.get("user_name") or payload.get("username") or "").strip()
    h = str(payload.get("user_hash") or "").strip()
    actor = name or h
    return actor, h

# =============================================================================
# Vault encryption (Fernet)
# =============================================================================

def _derive_fernet_key_from_master(master: str) -> bytes:
    digest = hashlib.sha256((master or "").encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


def _get_fernet():
    global _fernet_singleton
    if _fernet_singleton is not None:
        return _fernet_singleton
    if Fernet is None:
        raise RuntimeError("cryptography.fernet not available")

    # Preferred: explicit fernet key
    env_key = (os.getenv("SUPERTABLE_VAULT_FERNET_KEY") or "").strip()
    if env_key:
        _fernet_singleton = Fernet(env_key.encode("utf-8"))
        return _fernet_singleton

    # Fallback: derive from master secret
    master = (os.getenv("SUPERTABLE_VAULT_MASTER_KEY") or "").strip()
    if not master:
        # Common existing secrets in similar services
        for k in ("SUPERTABLE_SECRET_KEY", "SUPERTABLE_AUTH_SECRET", "SUPERTABLE_JWT_SECRET", "SUPERTABLE_SESSION_SECRET"):
            v = (os.getenv(k) or "").strip()
            if v:
                master = v
                break

    if not master:
        raise RuntimeError(
            "Vault encryption key missing. Set SUPERTABLE_VAULT_FERNET_KEY (preferred) "
            "or SUPERTABLE_VAULT_MASTER_KEY (or provide SUPERTABLE_SECRET_KEY / SUPERTABLE_AUTH_SECRET etc.)."
        )

    _fernet_singleton = Fernet(_derive_fernet_key_from_master(master))
    return _fernet_singleton


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


# =============================================================================
# Stages + Environments (Redis storage keys)
# =============================================================================

def _default_stages() -> list[str]:
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


# New (Vault) keys
def _stages_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:vault:stages"


def _envs_meta_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:vault:envs:meta"


def _env_meta_key(org: str, sup: str, name: str) -> str:
    return f"supertable:{org}:{sup}:meta:vault:envs:{_safe_slug(name)}:meta"


def _env_item_key(org: str, sup: str, name: str, stage: str) -> str:
    return f"supertable:{org}:{sup}:meta:vault:envs:{_safe_slug(name)}:{_safe_slug(stage)}"


# Legacy (environments.py) keys — we read & migrate automatically
def _legacy_stages_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:envs:stages"


def _legacy_envs_meta_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:envs:meta"


def _legacy_env_meta_key(org: str, sup: str, name: str) -> str:
    return f"supertable:{org}:{sup}:meta:envs:{_safe_slug(name)}:meta"


def _legacy_env_item_key(org: str, sup: str, name: str, stage: str) -> str:
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
    if not _safe_slug(name):
        raise HTTPException(status_code=400, detail="invalid name")
    return name


def _validate_dotenv(content: str) -> None:
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

        # remove inline comments for unquoted values
        if v and v[0] not in ("'", '"'):
            v = re.split(r"\s+#", v, maxsplit=1)[0].rstrip()

        # unquote
        if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
            q = v[0]
            inner = v[1:-1]
            if q == '"':
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


def _get_stages(org: str, sup: str) -> list[str]:
    """
    Returns stages from Vault key; if missing, reads legacy environments.py stages and migrates.
    """
    r = _get_redis()

    raw = r.get(_stages_key(org, sup))
    if raw:
        try:
            js = json.loads(_b2s(raw))
            if isinstance(js, list):
                out: list[str] = []
                for it in js:
                    s = _safe_slug(str(it or "").strip())
                    if s and s not in out:
                        out.append(s)
                if out:
                    return out
        except Exception:
            pass

    # legacy fallback
    raw_legacy = r.get(_legacy_stages_key(org, sup))
    if raw_legacy:
        try:
            js = json.loads(_b2s(raw_legacy))
            if isinstance(js, list):
                out: list[str] = []
                for it in js:
                    s = _safe_slug(str(it or "").strip())
                    if s and s not in out:
                        out.append(s)
                if out:
                    # migrate to new
                    r.set(_stages_key(org, sup), json.dumps(out, ensure_ascii=False))
                    return out
        except Exception:
            pass

    out = _default_stages()
    # store default for consistency
    r.set(_stages_key(org, sup), json.dumps(out, ensure_ascii=False))
    return out


def _ensure_env_meta_migrated(org: str, sup: str) -> None:
    """
    If legacy env names exist, copy them into the Vault meta set.
    """
    r = _get_redis()
    legacy_names = r.smembers(_legacy_envs_meta_key(org, sup)) or set()
    if not legacy_names:
        return
    for nm in legacy_names:
        nms = _b2s(nm)
        if nms:
            r.sadd(_envs_meta_key(org, sup), nms)


def _read_env_blob(org: str, sup: str, name: str, stage: str) -> Optional[Dict[str, Any]]:
    """
    Reads stage-scoped env record. If not found under Vault keys, tries legacy keys and migrates.
    Returns JSON dict (with ciphertext) or None.
    """
    r = _get_redis()
    key_new = _env_item_key(org, sup, name, stage)
    raw = r.get(key_new)
    if raw:
        try:
            js = json.loads(_b2s(raw))
            return js if isinstance(js, dict) else None
        except Exception:
            raise HTTPException(status_code=500, detail="corrupt vault entry")

    # Try legacy envs key
    key_legacy = _legacy_env_item_key(org, sup, name, stage)
    raw_legacy = r.get(key_legacy)
    if not raw_legacy:
        return None

    try:
        js_legacy = json.loads(_b2s(raw_legacy))
        if not isinstance(js_legacy, dict):
            return None
    except Exception:
        raise HTTPException(status_code=500, detail="corrupt legacy env entry")

    ciphertext = str(js_legacy.get("ciphertext") or "")
    if ciphertext:
        # validate decrypt + re-encrypt to ensure current Vault key is used
        content = _vault_decrypt(ciphertext)
        ciphertext_new = _vault_encrypt(content)
    else:
        content = ""
        ciphertext_new = _vault_encrypt(content)

    # migrate into Vault keyspace
    now = float(js_legacy.get("updated_at") or time.time())
    updated_by = str(js_legacy.get("updated_by") or "")
    note = str(js_legacy.get("note") or "")
    migrated = {
        "name": name,
        "stage": stage,
        "ciphertext": ciphertext_new,
        "updated_at": now,
        "updated_by": updated_by,
        "note": note,
        "migrated_from": "legacy_envs",
        "migrated_at": time.time(),
    }
    r.set(key_new, json.dumps(migrated, ensure_ascii=False))
    r.sadd(_envs_meta_key(org, sup), name)

    meta_key = _env_meta_key(org, sup, name)
    if not r.get(meta_key):
        legacy_meta = r.get(_legacy_env_meta_key(org, sup, name))
        if legacy_meta:
            r.set(meta_key, _b2s(legacy_meta))
        else:
            r.set(meta_key, json.dumps({"created_at": time.time(), "created_by": updated_by}, ensure_ascii=False))

    return migrated


# =============================================================================
# Export links (opaque tokens stored in Redis, supports rotation)
# =============================================================================

def _link_meta_key(org: str, sup: str, name: str, stage: str) -> str:
    return f"supertable:{org}:{sup}:meta:vault:links:{_safe_slug(name)}:{_safe_slug(stage)}"


def _link_token_key(token: str) -> str:
    return f"supertable:meta:vault:link:{_safe_slug(token)}"


def _make_token() -> str:
    return secrets.token_urlsafe(32)


def _link_get(org: str, sup: str, name: str, stage: str) -> Dict[str, Any]:
    r = _get_redis()
    raw = r.get(_link_meta_key(org, sup, name, stage))
    if not raw:
        return {"primary": "", "secondary": "", "updated_at": 0.0, "rotations": 0}
    try:
        js = json.loads(_b2s(raw))
        if not isinstance(js, dict):
            return {"primary": "", "secondary": "", "updated_at": 0.0, "rotations": 0}
        js.setdefault("primary", "")
        js.setdefault("secondary", "")
        js.setdefault("updated_at", 0.0)
        js.setdefault("rotations", 0)
        return js
    except Exception:
        return {"primary": "", "secondary": "", "updated_at": 0.0, "rotations": 0}


def _link_write(org: str, sup: str, name: str, stage: str, primary: str, secondary: str, rotations: int) -> Dict[str, Any]:
    r = _get_redis()
    meta = {"primary": primary or "", "secondary": secondary or "", "updated_at": time.time(), "rotations": int(rotations or 0)}
    r.set(_link_meta_key(org, sup, name, stage), json.dumps(meta, ensure_ascii=False))
    return meta


def _link_bind_token(token: str, org: str, sup: str, name: str, stage: str, kind: str) -> None:
    r = _get_redis()
    rec = {"org": org, "sup": sup, "name": name, "stage": stage, "kind": kind, "created_at": time.time()}
    r.set(_link_token_key(token), json.dumps(rec, ensure_ascii=False))


def _link_unbind_token(token: str) -> None:
    r = _get_redis()
    r.delete(_link_token_key(token))


def _ensure_primary_link(org: str, sup: str, name: str, stage: str) -> Dict[str, Any]:
    cur = _link_get(org, sup, name, stage)
    primary = str(cur.get("primary") or "")
    secondary = str(cur.get("secondary") or "")
    rotations = int(cur.get("rotations") or 0)
    if primary:
        return cur
    primary = _make_token()
    _link_bind_token(primary, org, sup, name, stage, "primary")
    return _link_write(org, sup, name, stage, primary, secondary, rotations)


def _rotate_primary_link(org: str, sup: str, name: str, stage: str) -> Dict[str, Any]:
    cur = _ensure_primary_link(org, sup, name, stage)
    old_primary = str(cur.get("primary") or "")
    old_secondary = str(cur.get("secondary") or "")
    rotations = int(cur.get("rotations") or 0)

    new_primary = _make_token()
    _link_bind_token(new_primary, org, sup, name, stage, "primary")

    # revoke old secondary, shift old primary -> secondary
    if old_secondary:
        _link_unbind_token(old_secondary)
    if old_primary:
        _link_bind_token(old_primary, org, sup, name, stage, "secondary")

    return _link_write(org, sup, name, stage, new_primary, old_primary, rotations + 1)


def _revoke_secondary_link(org: str, sup: str, name: str, stage: str) -> Dict[str, Any]:
    cur = _ensure_primary_link(org, sup, name, stage)
    primary = str(cur.get("primary") or "")
    secondary = str(cur.get("secondary") or "")
    rotations = int(cur.get("rotations") or 0)
    if secondary:
        _link_unbind_token(secondary)
    return _link_write(org, sup, name, stage, primary, "", rotations)


def _export_url(token: str) -> str:
    return f"/reflection/vault/envs/export?token={token}"


def _resolve_token(token: str) -> Optional[Dict[str, Any]]:
    r = _get_redis()
    raw = r.get(_link_token_key(token))
    if not raw:
        return None
    try:
        js = json.loads(_b2s(raw))
        return js if isinstance(js, dict) else None
    except Exception:
        return None


# =============================================================================
# Routes
# =============================================================================

def attach_vault_routes(
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
    """Attach Vault UI + API routes."""

    def _logged_dep(request: Request):
        return logged_in_guard_api(request)

    def _admin_dep(request: Request):
        return admin_guard_api(request)

    @router.get("/reflection/vault", response_class=HTMLResponse)
    def vault_page(request: Request, org: Optional[str] = Query(None), sup: Optional[str] = Query(None)):
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
        resp = templates.TemplateResponse("vault.html", ctx)
        no_store(resp)
        return resp

    # -------------------------------------------------------------------------
    # Stages
    # -------------------------------------------------------------------------

    @router.get("/reflection/vault/stages")
    def vault_stages(org: str, sup: str, _: Any = Depends(_logged_dep)):
        org = str(org or "").strip()
        sup = str(sup or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")
        stages = _get_stages(org, sup)
        return {"ok": True, "stages": stages}

    @router.post("/reflection/vault/stages/add")
    def vault_stage_add(payload: Dict[str, Any] = Body(...), _: Any = Depends(_admin_dep)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        stage = _validate_stage_name(payload.get("stage"))
        actor, actor_hash = _actor_from_payload(payload)
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        r = _get_redis()
        stages = _get_stages(org, sup)
        if stage not in stages:
            stages.append(stage)
            r.set(_stages_key(org, sup), json.dumps(stages, ensure_ascii=False))
        return {"ok": True, "stages": stages, "updated_by": actor, "updated_by_hash": actor_hash, "updated_at": time.time()}

    @router.post("/reflection/vault/stages/delete")
    def vault_stage_delete(payload: Dict[str, Any] = Body(...), _: Any = Depends(_admin_dep)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        stage = _validate_stage_name(payload.get("stage"))
        actor, actor_hash = _actor_from_payload(payload)
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        r = _get_redis()
        stages = _get_stages(org, sup)
        if stage not in stages:
            return {"ok": True, "stages": stages}
        if len(stages) <= 1:
            raise HTTPException(status_code=400, detail="cannot delete the last stage")

        stages = [s for s in stages if s != stage]
        r.set(_stages_key(org, sup), json.dumps(stages, ensure_ascii=False))

        # Remove per-stage values and links for all envs
        _ensure_env_meta_migrated(org, sup)
        env_names = [_b2s(n) for n in (r.smembers(_envs_meta_key(org, sup)) or [])]
        for name in env_names:
            if not name:
                continue
            r.delete(_env_item_key(org, sup, name, stage))

            # also legacy cleanup
            r.delete(_legacy_env_item_key(org, sup, name, stage))

            meta = _link_get(org, sup, name, stage)
            for t in (meta.get("primary") or "", meta.get("secondary") or ""):
                if t:
                    _link_unbind_token(str(t))
            r.delete(_link_meta_key(org, sup, name, stage))

        return {"ok": True, "stages": stages, "updated_by": actor, "updated_by_hash": actor_hash, "updated_at": time.time()}

    # -------------------------------------------------------------------------
    # Environments
    # -------------------------------------------------------------------------

    @router.get("/reflection/vault/envs/list")
    def envs_list(org: str, sup: str, _: Any = Depends(_logged_dep)):
        org = str(org or "").strip()
        sup = str(sup or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        r = _get_redis()
        stages = _get_stages(org, sup)

        # merge legacy env names into new meta
        _ensure_env_meta_migrated(org, sup)

        names = [_b2s(n) for n in (r.smembers(_envs_meta_key(org, sup)) or [])]
        names = [n for n in names if n]
        names.sort()

        items: list[Dict[str, Any]] = []
        for name in names:
            stage_meta: Dict[str, Any] = {}
            last_at = 0.0
            last_by = ""
            for st in stages:
                js = _read_env_blob(org, sup, name, st)
                if not js:
                    continue
                at = float(js.get("updated_at") or 0.0)
                by = str(js.get("updated_by") or "")
                stage_meta[st] = {"updated_at": at, "updated_by": by, "note": js.get("note") or ""}
                if at >= last_at:
                    last_at = at
                    last_by = by

            items.append({"name": name, "last_updated_at": last_at, "last_updated_by": last_by, "stages": stage_meta})

        items.sort(key=lambda x: float(x.get("last_updated_at") or 0.0), reverse=True)
        return {"ok": True, "stages": stages, "items": items}

    @router.post("/reflection/vault/envs/get")
    def envs_get(payload: Dict[str, Any] = Body(...), _: Any = Depends(_logged_dep)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        name = _validate_env_name(payload.get("name"))
        stage = _validate_stage_name(payload.get("stage"))
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        js = _read_env_blob(org, sup, name, stage)
        links = _ensure_primary_link(org, sup, name, stage)

        if not js:
            return {
                "ok": True,
                "name": name,
                "stage": stage,
                "content": "",
                "found": False,
                "links": {
                    "primary": {"token": links.get("primary") or "", "url": _export_url(links.get("primary") or "") if links.get("primary") else ""},
                    "secondary": {"token": links.get("secondary") or "", "url": _export_url(links.get("secondary") or "") if links.get("secondary") else ""},
                    "rotations": int(links.get("rotations") or 0),
                },
            }

        ciphertext = str(js.get("ciphertext") or "")
        content = _vault_decrypt(ciphertext) if ciphertext else ""

        return {
            "ok": True,
            "name": name,
            "stage": stage,
            "content": content,
            "found": True,
            "updated_at": js.get("updated_at"),
            "updated_by": js.get("updated_by"),
            "note": js.get("note") or "",
            "links": {
                "primary": {"token": links.get("primary") or "", "url": _export_url(links.get("primary") or "") if links.get("primary") else ""},
                "secondary": {"token": links.get("secondary") or "", "url": _export_url(links.get("secondary") or "") if links.get("secondary") else ""},
                "rotations": int(links.get("rotations") or 0),
            },
        }

    @router.post("/reflection/vault/envs/create")
    def envs_create(payload: Dict[str, Any] = Body(...), _: Any = Depends(_admin_dep)):
        """
        Creates the value for (name, stage). If the environment exists already, it still allows
        creating a missing stage value. It only conflicts if this specific stage value exists.
        """
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        name = _validate_env_name(payload.get("name"))
        stage = _validate_stage_name(payload.get("stage"))
        content = str(payload.get("content") or "")
        note = str(payload.get("note") or "")
        actor, actor_hash = _actor_from_payload(payload)
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        _validate_dotenv(content)

        stages = _get_stages(org, sup)
        if stage not in stages:
            raise HTTPException(status_code=400, detail="unknown stage")

        r = _get_redis()
        # If legacy exists, migrate meta so update works smoothly
        _ensure_env_meta_migrated(org, sup)

        if r.exists(_env_item_key(org, sup, name, stage)):
            raise HTTPException(status_code=409, detail="environment already exists for this stage")

        now = time.time()
        ciphertext = _vault_encrypt(content)
        item = {"name": name, "stage": stage, "ciphertext": ciphertext, "updated_at": now, "updated_by": actor, "updated_by_hash": actor_hash, "note": note}
        r.set(_env_item_key(org, sup, name, stage), json.dumps(item, ensure_ascii=False))
        r.sadd(_envs_meta_key(org, sup), name)

        meta_key = _env_meta_key(org, sup, name)
        if not r.get(meta_key):
            # migrate legacy meta if exists
            legacy_meta = r.get(_legacy_env_meta_key(org, sup, name))
            if legacy_meta:
                r.set(meta_key, _b2s(legacy_meta))
            else:
                r.set(meta_key, json.dumps({"created_at": now, "created_by": actor}, ensure_ascii=False))

        links = _ensure_primary_link(org, sup, name, stage)
        return {
            "ok": True,
            "name": name,
            "stage": stage,
            "updated_at": now,
            "updated_by": actor, "updated_by_hash": actor_hash,
            "links": {
                "primary": {"token": links.get("primary") or "", "url": _export_url(links.get("primary") or "") if links.get("primary") else ""},
                "secondary": {"token": links.get("secondary") or "", "url": _export_url(links.get("secondary") or "") if links.get("secondary") else ""},
                "rotations": int(links.get("rotations") or 0),
            },
        }

    @router.post("/reflection/vault/envs/update")
    def envs_update(payload: Dict[str, Any] = Body(...), _: Any = Depends(_admin_dep)):
        """
        Updates (name, stage). Upserts stage value as long as the environment exists.
        """
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        name = _validate_env_name(payload.get("name"))
        stage = _validate_stage_name(payload.get("stage"))
        content = str(payload.get("content") or "")
        note = str(payload.get("note") or "")
        actor, actor_hash = _actor_from_payload(payload)
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        _validate_dotenv(content)

        r = _get_redis()
        _ensure_env_meta_migrated(org, sup)

        if not r.sismember(_envs_meta_key(org, sup), name):
            # legacy membership?
            if r.sismember(_legacy_envs_meta_key(org, sup), name):
                r.sadd(_envs_meta_key(org, sup), name)
            else:
                raise HTTPException(status_code=404, detail="environment not found")

        stages = _get_stages(org, sup)
        if stage not in stages:
            raise HTTPException(status_code=400, detail="unknown stage")

        now = time.time()
        ciphertext = _vault_encrypt(content)
        item = {"name": name, "stage": stage, "ciphertext": ciphertext, "updated_at": now, "updated_by": actor, "updated_by_hash": actor_hash, "note": note}
        r.set(_env_item_key(org, sup, name, stage), json.dumps(item, ensure_ascii=False))

        links = _ensure_primary_link(org, sup, name, stage)
        return {
            "ok": True,
            "name": name,
            "stage": stage,
            "updated_at": now,
            "updated_by": actor, "updated_by_hash": actor_hash,
            "links": {
                "primary": {"token": links.get("primary") or "", "url": _export_url(links.get("primary") or "") if links.get("primary") else ""},
                "secondary": {"token": links.get("secondary") or "", "url": _export_url(links.get("secondary") or "") if links.get("secondary") else ""},
                "rotations": int(links.get("rotations") or 0),
            },
        }

    @router.delete("/reflection/vault/envs/{name}")
    def envs_delete(name: str, org: str, sup: str, stage: Optional[str] = Query(None), _: Any = Depends(_admin_dep)):
        org = str(org or "").strip()
        sup = str(sup or "").strip()
        name = _validate_env_name(name)
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        r = _get_redis()
        stages = _get_stages(org, sup)
        _ensure_env_meta_migrated(org, sup)

        stage_q = str(stage or "").strip() or None
        if stage_q:
            stage_name = _validate_stage_name(stage_q)
            if stage_name not in stages:
                raise HTTPException(status_code=400, detail="unknown stage")

            r.delete(_env_item_key(org, sup, name, stage_name))
            r.delete(_legacy_env_item_key(org, sup, name, stage_name))

            has_any = False
            for st in stages:
                if r.exists(_env_item_key(org, sup, name, st)) or r.exists(_legacy_env_item_key(org, sup, name, st)):
                    has_any = True
                    break

            if not has_any:
                r.delete(_env_meta_key(org, sup, name))
                r.delete(_legacy_env_meta_key(org, sup, name))
                r.srem(_envs_meta_key(org, sup), name)
                r.srem(_legacy_envs_meta_key(org, sup), name)
                for st in stages:
                    meta = _link_get(org, sup, name, st)
                    for t in (meta.get("primary") or "", meta.get("secondary") or ""):
                        if t:
                            _link_unbind_token(str(t))
                    r.delete(_link_meta_key(org, sup, name, st))
            return {"ok": True, "deleted_stage": stage_name, "remaining": has_any}

        for st in stages:
            r.delete(_env_item_key(org, sup, name, st))
            r.delete(_legacy_env_item_key(org, sup, name, st))
            meta = _link_get(org, sup, name, st)
            for t in (meta.get("primary") or "", meta.get("secondary") or ""):
                if t:
                    _link_unbind_token(str(t))
            r.delete(_link_meta_key(org, sup, name, st))

        r.delete(_env_meta_key(org, sup, name))
        r.delete(_legacy_env_meta_key(org, sup, name))
        r.srem(_envs_meta_key(org, sup), name)
        r.srem(_legacy_envs_meta_key(org, sup), name)
        return {"ok": True}

    # -------------------------------------------------------------------------
    # Link management
    # -------------------------------------------------------------------------

    @router.post("/reflection/vault/envs/link/rotate")
    def envs_link_rotate(payload: Dict[str, Any] = Body(...), _: Any = Depends(_admin_dep)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        name = _validate_env_name(payload.get("name"))
        stage = _validate_stage_name(payload.get("stage"))
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        links = _rotate_primary_link(org, sup, name, stage)
        p = str(links.get("primary") or "")
        s = str(links.get("secondary") or "")
        return {"ok": True, "name": name, "stage": stage, "rotations": int(links.get("rotations") or 0),
                "primary": {"token": p, "url": _export_url(p) if p else ""},
                "secondary": {"token": s, "url": _export_url(s) if s else ""}}

    @router.post("/reflection/vault/envs/link/revoke_secondary")
    def envs_link_revoke_secondary(payload: Dict[str, Any] = Body(...), _: Any = Depends(_admin_dep)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        name = _validate_env_name(payload.get("name"))
        stage = _validate_stage_name(payload.get("stage"))
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")

        links = _revoke_secondary_link(org, sup, name, stage)
        p = str(links.get("primary") or "")
        s = str(links.get("secondary") or "")
        return {"ok": True, "name": name, "stage": stage, "rotations": int(links.get("rotations") or 0),
                "primary": {"token": p, "url": _export_url(p) if p else ""},
                "secondary": {"token": s, "url": _export_url(s) if s else ""}}

    @router.get("/reflection/vault/envs/export")
    def envs_export(token: str):
        data = _resolve_token(token)
        if not data:
            raise HTTPException(status_code=401, detail="invalid token")

        org = str(data.get("org") or "").strip()
        sup = str(data.get("sup") or "").strip()
        name = _validate_env_name(data.get("name"))
        stage = _validate_stage_name(data.get("stage"))
        if not org or not sup:
            raise HTTPException(status_code=401, detail="invalid token scope")

        js = _read_env_blob(org, sup, name, stage)
        if not js:
            return {"ok": True, "found": False, "name": name, "stage": stage, "vars": {}, "items": []}

        ciphertext = str(js.get("ciphertext") or "")
        if not ciphertext:
            return {"ok": True, "found": True, "name": name, "stage": stage, "vars": {}, "items": []}

        content = _vault_decrypt(ciphertext)
        vars_dict = _dotenv_parse_vars(content)
        items = [{"key": k, "value": v} for k, v in vars_dict.items()]
        return {"ok": True, "found": True, "name": name, "stage": stage, "vars": vars_dict, "items": items}
