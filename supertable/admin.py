from __future__ import annotations

import os
import json
import html
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional, Tuple, Set
from pathlib import Path

import redis
from fastapi import FastAPI, Query, HTTPException, Request, Depends, Form
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

# ------------------------------ Settings ------------------------------

class Settings:
    def __init__(self) -> None:
        self.REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
        self.REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
        self.REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
        self.REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD")
        self.ADMIN_TOKEN: Optional[str] = os.getenv("ADMIN_TOKEN")
        self.DOTENV_PATH: str = os.getenv("DOTENV_PATH", ".env")
        self.TEMPLATES_DIR: str = os.getenv(
            "TEMPLATES_DIR",
            str(Path(__file__).resolve().parent / "templates")
        )

settings = Settings()

# ------------------------------ Catalog (import or fallback) ------------------------------

def _now_ms() -> int:
    from time import time as _t
    return int(_t() * 1000)

def _root_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:root"

def _leaf_key(org: str, sup: str, simple: str) -> str:
    return f"supertable:{org}:{sup}:meta:leaf:{simple}"

def _mirrors_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:mirrors"

class _FallbackCatalog:
    def __init__(self, r: redis.Redis):
        self.r = r

    def ensure_root(self, org: str, sup: str) -> None:
        key = _root_key(org, sup)
        if not self.r.exists(key):
            self.r.set(key, json.dumps({"version": 0, "ts": _now_ms()}))

    def get_root(self, org: str, sup: str) -> Optional[Dict]:
        raw = self.r.get(_root_key(org, sup))
        return json.loads(raw) if raw else None

    def get_leaf(self, org: str, sup: str, simple: str) -> Optional[Dict]:
        raw = self.r.get(_leaf_key(org, sup, simple))
        return json.loads(raw) if raw else None

    def get_mirrors(self, org: str, sup: str) -> List[str]:
        raw = self.r.get(_mirrors_key(org, sup))
        if not raw:
            return []
        try:
            obj = json.loads(raw)
        except Exception:
            return []
        out = []
        for f in (obj.get("formats") or []):
            fu = str(f).upper()
            if fu in ("DELTA", "ICEBERG", "PARQUET") and fu not in out:
                out.append(fu)
        return out

    def set_mirrors(self, org: str, sup: str, formats: List[str]) -> List[str]:
        uniq = []
        for f in (formats or []):
            fu = str(f).upper()
            if fu in ("DELTA", "ICEBERG", "PARQUET") and fu not in uniq:
                uniq.append(fu)
        self.r.set(_mirrors_key(org, sup), json.dumps({"formats": uniq, "ts": _now_ms()}))
        return uniq

    def enable_mirror(self, org: str, sup: str, fmt: str) -> List[str]:
        cur = self.get_mirrors(org, sup)
        fu = str(fmt).upper()
        if fu not in ("DELTA", "ICEBERG", "PARQUET") or fu in cur:
            return cur
        return self.set_mirrors(org, sup, cur + [fu])

    def disable_mirror(self, org: str, sup: str, fmt: str) -> List[str]:
        cur = self.get_mirrors(org, sup)
        fu = str(fmt).upper()
        nxt = [x for x in cur if x != fu]
        return self.set_mirrors(org, sup, nxt)

    def scan_leaf_keys(self, org: str, sup: str, count: int = 1000) -> Iterator[str]:
        pattern = f"supertable:{org}:{sup}:meta:leaf:*"
        cursor = 0
        while True:
            cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=max(1, int(count)))
            for k in keys:
                yield k if isinstance(k, str) else k.decode("utf-8")
            if cursor == 0:
                break

    def scan_leaf_items(self, org: str, sup: str, count: int = 1000) -> Iterator[Dict]:
        batch: List[str] = []
        for key in self.scan_leaf_keys(org, sup, count=count):
            batch.append(key)
            if len(batch) >= count:
                yield from self._fetch_batch(batch)
                batch = []
        if batch:
            yield from self._fetch_batch(batch)

    def _fetch_batch(self, keys: List[str]) -> Iterator[Dict]:
        pipe = self.r.pipeline()
        for k in keys:
            pipe.get(k)
        vals = pipe.execute()
        for k, raw in zip(keys, vals):
            if not raw:
                continue
            try:
                obj = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                simple = k.rsplit("meta:leaf:", 1)[-1]
                yield {
                    "simple": simple,
                    "version": int(obj.get("version", -1)),
                    "ts": int(obj.get("ts", 0)),
                    "path": obj.get("path", ""),
                }
            except Exception:
                continue

def _build_catalog() -> Tuple[object, redis.Redis]:
    r = redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        password=settings.REDIS_PASSWORD,
        decode_responses=True,
    )
    try:
        from supertable.redis_catalog import RedisCatalog as _RC  # type: ignore
        return _RC(), r
    except Exception:
        try:
            from redis_catalog import RedisCatalog as _RC  # type: ignore
            return _RC(), r
        except Exception:
            return _FallbackCatalog(r), r

catalog, redis_client = _build_catalog()

# ------------------------------ Discovery & Utils ------------------------------

def discover_pairs(limit_pairs: int = 10000) -> List[Tuple[str, str]]:
    pairs: Set[Tuple[str, str]] = set()
    cursor = 0
    pattern = "supertable:*:*:meta:*"
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)
        for k in keys:
            s = k if isinstance(k, str) else k.decode("utf-8")
            parts = s.split(":")
            if len(parts) >= 5 and parts[0] == "supertable" and parts[3] == "meta":
                pairs.add((parts[1], parts[2]))
                if len(pairs) >= limit_pairs:
                    break
        if cursor == 0 or len(pairs) >= limit_pairs:
            break
    return sorted(pairs)

def resolve_pair(org: Optional[str], sup: Optional[str]) -> Tuple[str, str]:
    pairs = discover_pairs()
    if org and sup:
        return org, sup
    if org and not sup:
        for o, s in pairs:
            if o == org:
                return o, s
    if sup and not org:
        for o, s in pairs:
            if s == sup:
                return o, s
    if not pairs:
        return "", ""
    return pairs[0]

def _fmt_ts(ms: int) -> str:
    if not ms:
        return "—"
    try:
        dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return str(ms)

def _escape(s: str) -> str:
    return html.escape(str(s or ""), quote=True)

# ------------------------------ Security helpers ------------------------------

def _get_provided_token(request: Request, token_param: Optional[str]) -> Optional[str]:
    if token_param:
        return token_param
    hdr = request.headers.get("X-Admin-Token")
    if hdr:
        return hdr
    cookie = request.cookies.get("st_admin_token")
    if cookie:
        return cookie
    return None

def admin_guard_api(token: Optional[str] = Query(None), request: Request = None):
    required = settings.ADMIN_TOKEN
    if not required:
        return True
    provided = _get_provided_token(request, token)
    if provided != required:
        raise HTTPException(status_code=401, detail="Invalid or missing admin token")
    return True

# ------------------------------ Direct Redis readers (users/roles) ------------------------------

def _r_type(key: str) -> str:
    try:
        return redis_client.type(key)
    except Exception:
        return "none"

def _read_string_json(key: str) -> Optional[Dict]:
    raw = redis_client.get(key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return {"value": raw}

def _read_hash(key: str) -> Optional[Dict]:
    try:
        data = redis_client.hgetall(key)
        return data or None
    except Exception:
        return None

def list_users(org: str, sup: str) -> List[Dict]:
    out: List[Dict] = []
    pattern = f"supertable:{org}:{sup}:meta:users:*"
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=500)
        for key in keys:
            k = key if isinstance(key, str) else key.decode("utf-8")
            tail = k.rsplit(":", 1)[-1]
            if tail in ("meta", "name_to_hash"):
                continue
            t = _r_type(k)
            doc = None
            if t == "string":
                doc = _read_string_json(k)
            elif t == "hash":
                doc = _read_hash(k)
            else:
                continue
            if doc is None:
                continue
            name = doc.get("name") if isinstance(doc, dict) else None
            roles = doc.get("roles") if isinstance(doc, dict) else None
            if isinstance(roles, str):
                try:
                    roles = json.loads(roles)
                except Exception:
                    roles = [roles]
            if roles is None:
                roles = []
            out.append({"hash": tail, **doc, "name": name, "roles": roles})
        if cursor == 0:
            break
    return out

def list_roles(org: str, sup: str) -> List[Dict]:
    out: List[Dict] = []
    pattern = f"supertable:{org}:{sup}:meta:roles:*"
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=500)
        for key in keys:
            k = key if isinstance(key, str) else key.decode("utf-8")
            if ":type_to_hash:" in k:
                continue
            tail = k.rsplit(":", 1)[-1]
            if tail == "meta":
                continue
            t = _r_type(k)
            doc = None
            if t == "string":
                doc = _read_string_json(k)
            elif t == "hash":
                doc = _read_hash(k)
            else:
                continue
            if doc is None:
                continue
            out.append({"hash": tail, **doc})
        if cursor == 0:
            break
    return out

def read_user(org: str, sup: str, user_hash: str) -> Optional[Dict]:
    k = f"supertable:{org}:{sup}:meta:users:{user_hash}"
    t = _r_type(k)
    if t == "string":
        return _read_string_json(k)
    if t == "hash":
        return _read_hash(k)
    return None

def read_role(org: str, sup: str, role_hash: str) -> Optional[Dict]:
    k = f"supertable:{org}:{sup}:meta:roles:{role_hash}"
    t = _r_type(k)
    if t == "string":
        return _read_string_json(k)
    if t == "hash":
        return _read_hash(k)
    return None

# ------------------------------ App + templates ------------------------------

app = FastAPI(title="SuperTable Redis Admin", version="1.6.0")
templates = Jinja2Templates(directory=settings.TEMPLATES_DIR)

@app.get("/favicon.ico")
def favicon():
    # prevent browsers from re-requesting and causing noisy logs
    return Response(status_code=204)

@app.get("/healthz", response_class=PlainTextResponse)
def healthz():
    try:
        pong = redis_client.ping()
        return "ok" if pong else "not-ok"
    except Exception as e:
        return f"error: {e}"

# -------- JSON API (read-only) --------

@app.get("/api/tenants")
def api_tenants():
    pairs = discover_pairs()
    return {"tenants": [{"org": o, "sup": s} for o, s in pairs]}

@app.get("/api/root")
def api_get_root(org: Optional[str] = Query(None), sup: Optional[str] = Query(None)):
    org, sup = resolve_pair(org, sup)
    if not org or not sup:
        return {"org": org, "sup": sup, "root": None}
    if hasattr(catalog, "ensure_root"):
        try:
            catalog.ensure_root(org, sup)
        except Exception:
            pass
    try:
        root = catalog.get_root(org, sup)
    except Exception:
        root = None
    return {"org": org, "sup": sup, "root": root}

@app.get("/api/mirrors")
def api_get_mirrors(org: Optional[str] = Query(None), sup: Optional[str] = Query(None)):
    org, sup = resolve_pair(org, sup)
    if not org or not sup:
        return {"org": org, "sup": sup, "formats": []}
    try:
        fmts = catalog.get_mirrors(org, sup)
    except Exception:
        fmts = []
    return {"org": org, "sup": sup, "formats": fmts}

@app.get("/api/leaves")
def api_list_leaves(
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    org, sup = resolve_pair(org, sup)
    if not org or not sup:
        return {"org": org, "sup": sup, "total": 0, "page": page, "page_size": page_size, "items": []}

    items: List[Dict] = []
    total = 0
    ql = (q or "").lower()

    scan_iter = None
    if hasattr(catalog, "scan_leaf_items"):
        try:
            scan_iter = catalog.scan_leaf_items(org, sup, count=1000)
        except Exception:
            scan_iter = None
    if scan_iter is None:
        pattern = f"supertable:{org}:{sup}:meta:leaf:*"
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)
            for key in keys:
                raw = redis_client.get(key)
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                except Exception:
                    continue
                simple = (key if isinstance(key, str) else key.decode("utf-8")).rsplit("meta:leaf:", 1)[-1]
                rec = {
                    "simple": simple,
                    "version": int(obj.get("version", -1)),
                    "ts": int(obj.get("ts", 0)),
                    "path": obj.get("path", ""),
                }
                if q and ql not in simple.lower():
                    continue
                total += 1
                items.append(rec)
            if cursor == 0:
                break
    else:
        for item in scan_iter:
            simple = item.get("simple", "")
            if q and ql not in simple.lower():
                continue
            total += 1
            items.append(item)

    items.sort(key=lambda x: x.get("simple", ""))

    start = (page - 1) * page_size
    end = start + page_size
    page_items = items[start:end]
    return {"org": org, "sup": sup, "total": total, "page": page, "page_size": page_size, "items": page_items}

@app.get("/api/leaf/{simple}")
def api_get_leaf(simple: str, org: Optional[str] = Query(None), sup: Optional[str] = Query(None)):
    org, sup = resolve_pair(org, sup)
    if not org or not sup:
        raise HTTPException(404, "Tenant not found")
    try:
        obj = catalog.get_leaf(org, sup, simple)
    except Exception:
        obj = None
    if not obj:
        raise HTTPException(status_code=404, detail="Leaf not found")
    return {"org": org, "sup": sup, "simple": simple, "data": obj}

@app.get("/api/users")
def api_users(org: Optional[str] = Query(None), sup: Optional[str] = Query(None), _=Depends(admin_guard_api)):
    org, sup = resolve_pair(org, sup)
    if not org or not sup:
        return {"users": []}
    return {"users": list_users(org, sup)}

@app.get("/api/roles")
def api_roles(org: Optional[str] = Query(None), sup: Optional[str] = Query(None), _=Depends(admin_guard_api)):
    org, sup = resolve_pair(org, sup)
    if not org or not sup:
        return {"roles": []}
    return {"roles": list_roles(org, sup)}

@app.get("/api/user/{user_hash}")
def api_user_details(user_hash: str, org: Optional[str] = Query(None), sup: Optional[str] = Query(None), _=Depends(admin_guard_api)):
    org, sup = resolve_pair(org, sup)
    if not org or not sup:
        raise HTTPException(404, "Tenant not found")
    obj = read_user(org, sup, user_hash)
    if not obj:
        raise HTTPException(status_code=404, detail="User not found")
    return {"hash": user_hash, "data": obj}

@app.get("/api/role/{role_hash}")
def api_role_details(role_hash: str, org: Optional[str] = Query(None), sup: Optional[str] = Query(None), _=Depends(admin_guard_api)):
    org, sup = resolve_pair(org, sup)
    if not org or not sup:
        raise HTTPException(404, "Tenant not found")
    obj = read_role(org, sup, role_hash)
    if not obj:
        raise HTTPException(status_code=404, detail="Role not found")
    return {"hash": role_hash, "data": obj}

# -------- Admin-only modify APIs --------

@app.post("/api/mirrors/enable")
def api_enable_mirror(fmt: str = Query(..., description="DELTA|ICEBERG|PARQUET"), org: Optional[str] = Query(None), sup: Optional[str] = Query(None), _=Depends(admin_guard_api)):
    org, sup = resolve_pair(org, sup)
    if not org or not sup:
        raise HTTPException(400, "No tenants discovered in Redis")
    try:
        formats = catalog.enable_mirror(org, sup, fmt)
    except Exception:
        raw = redis_client.get(_mirrors_key(org, sup))
        cur = []
        if raw:
            try:
                cur = list(dict.fromkeys([str(x).upper() for x in (json.loads(raw).get("formats") or [])]))
            except Exception:
                cur = []
        fu = str(fmt).upper()
        if fu not in ("DELTA","ICEBERG","PARQUET") or fu in cur:
            formats = cur
        else:
            uniq = cur + [fu]
            redis_client.set(_mirrors_key(org, sup), json.dumps({"formats": uniq, "ts": _now_ms()}))
            formats = uniq
    return {"org": org, "sup": sup, "formats": formats}

@app.post("/api/mirrors/disable")
def api_disable_mirror(fmt: str = Query(..., description="DELTA|ICEBERG|PARQUET"), org: Optional[str] = Query(None), sup: Optional[str] = Query(None), _=Depends(admin_guard_api)):
    org, sup = resolve_pair(org, sup)
    if not org or not sup:
        raise HTTPException(400, "No tenants discovered in Redis")
    try:
        formats = catalog.disable_mirror(org, sup, fmt)
    except Exception:
        raw = redis_client.get(_mirrors_key(org, sup))
        cur = []
        if raw:
            try:
                cur = list(dict.fromkeys([str(x).upper() for x in (json.loads(raw).get("formats") or [])]))
            except Exception:
                cur = []
        fu = str(fmt).upper()
        uniq = [x for x in cur if x != fu]
        redis_client.set(_mirrors_key(org, sup), json.dumps({"formats": uniq, "ts": _now_ms()}))
        formats = uniq
    return {"org": org, "sup": sup, "formats": formats}

# ------------------------------ .env helpers + pages ------------------------------

def _parse_dotenv(path: str) -> Dict[str, str]:
    env: Dict[str, str] = {}
    p = Path(path)
    if not p.exists() or not p.is_file():
        return env
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "=" not in s:
            continue
        k, v = s.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        env[k] = v
    return env

def _effective_settings() -> Dict[str, str]:
    keys = ["REDIS_HOST", "REDIS_PORT", "REDIS_DB", "REDIS_PASSWORD", "ADMIN_TOKEN", "DOTENV_PATH"]
    out = {}
    for k in keys:
        out[k] = os.getenv(k)
    return out

def _gate_or_login_page(request: Request, token: Optional[str]) -> Optional[HTMLResponse]:
    required = settings.ADMIN_TOKEN
    if not required:
        return None
    provided = _get_provided_token(request, token)
    if provided == required:
        return None
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/admin/login")
def admin_login(token: str = Form(...)):
    if not settings.ADMIN_TOKEN:
        return RedirectResponse("/admin", status_code=302)
    if token != settings.ADMIN_TOKEN:
        return HTMLResponse("<h3>Invalid token</h3><a href='/admin'>Back</a>", status_code=401)
    resp = RedirectResponse("/admin", status_code=302)
    resp.set_cookie("st_admin_token", token, httponly=True, samesite="lax", path="/", secure=False, max_age=7*24*3600)
    return resp

@app.get("/admin/logout")
def admin_logout():
    resp = RedirectResponse("/admin", status_code=302)
    resp.delete_cookie("st_admin_token", path="/")
    return resp

@app.get("/admin", response_class=HTMLResponse)
def admin_page(
    request: Request,
    token: Optional[str] = Query(None),
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=5, le=200),
):
    gate = _gate_or_login_page(request, token)
    if gate is not None:
        return gate

    provided = _get_provided_token(request, token) or ""

    pairs = discover_pairs()
    sel_org, sel_sup = resolve_pair(org, sup)

    tenants = [{"org": o, "sup": s, "selected": (o == sel_org and s == sel_sup)} for o, s in pairs]

    if not sel_org or not sel_sup:
        return templates.TemplateResponse("admin.html", {
            "request": request,
            "authorized": True,
            "token": provided,
            "tenants": tenants,
            "sel_org": sel_org,
            "sel_sup": sel_sup,
            "has_tenant": False,
        })

    # Root & mirrors
    try:
        root = catalog.get_root(sel_org, sel_sup) or {}
    except Exception:
        root = {}
    try:
        mirrors = catalog.get_mirrors(sel_org, sel_sup) or []
    except Exception:
        mirrors = []

    # Leaves
    listing = api_list_leaves(org=sel_org, sup=sel_sup, q=q, page=page, page_size=page_size)
    raw_items = listing["items"]
    items = [{**it, "ts_fmt": _fmt_ts(int(it.get("ts", 0)))} for it in raw_items]
    total = int(listing["total"])
    pages = (total + page_size - 1) // page_size if total else 1

    # Users & Roles
    users = list_users(sel_org, sel_sup)
    roles = list_roles(sel_org, sel_sup)

    ctx = {
        "request": request,
        "authorized": True,
        "token": provided,
        "tenants": tenants,
        "sel_org": sel_org,
        "sel_sup": sel_sup,
        "has_tenant": True,
        "root_version": int(root.get("version", -1)) if isinstance(root, dict) else -1,
        "root_ts": _fmt_ts(int(root.get("ts", 0))) if isinstance(root, dict) else "—",
        "mirrors": mirrors,
        "q": q or "",
        "page": page,
        "pages": pages if pages else 1,
        "total": total,
        "items": items,
        "users": users,
        "roles": roles,
    }
    return templates.TemplateResponse("admin.html", ctx)

@app.get("/admin/config", response_class=HTMLResponse)
def admin_config(request: Request, token: Optional[str] = Query(None)):
    gate = _gate_or_login_page(request, token)
    if gate is not None:
        return gate

    dotenv_path = settings.DOTENV_PATH
    dotenv_vars = _parse_dotenv(dotenv_path)
    effective = _effective_settings()

    all_keys = sorted(set(list(dotenv_vars.keys()) + list(effective.keys())))
    rows = [{
        "key": k,
        "env_val": dotenv_vars.get(k),
        "eff_val": effective.get(k),
        "is_sensitive": any(x in k.lower() for x in ("pass","token","secret","key")),
    } for k in all_keys]

    return templates.TemplateResponse("config.html", {
        "request": request,
        "dotenv_path": dotenv_path,
        "rows": rows,
    })

@app.get("/", response_class=HTMLResponse)
def root_redirect():
    # proper HTTP redirect (no meta refresh)
    return RedirectResponse("/admin", status_code=302)

# ------------------------------ Entrypoint ------------------------------

if __name__ == "__main__":
    import uvicorn
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    reload_flag = os.getenv("UVICORN_RELOAD", "0").lower() in ("1","true","yes","on")
    uvicorn.run(app, host=host, port=port, reload=reload_flag)
