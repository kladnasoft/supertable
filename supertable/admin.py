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

# Load environment variables from .env file
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    # dotenv not available, continue without it
    pass


# ------------------------------ Settings ------------------------------

class Settings:
    def __init__(self) -> None:
        self.REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
        self.REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
        self.REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
        self.REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD")
        self.SUPERTABLE_ADMIN_TOKEN: Optional[str] = os.getenv("SUPERTABLE_ADMIN_TOKEN")
        self.DOTENV_PATH: str = os.getenv("DOTENV_PATH", ".env")
        self.TEMPLATES_DIR: str = os.getenv(
            "TEMPLATES_DIR",
            str(Path(__file__).resolve().parent / "templates")
        )
        # set to 1 in HTTPS environments
        self.SECURE_COOKIES: bool = os.getenv("SECURE_COOKIES", "0").strip().lower() in ("1", "true", "yes", "on")


settings = Settings()


def _required_token() -> str:
    # Trim to avoid surprises from .env quoting/spacing
    return (settings.SUPERTABLE_ADMIN_TOKEN or "").strip()


def _now_ms() -> int:
    from time import time as _t
    return int(_t() * 1000)


# ------------------------------ Catalog (import or fallback) ------------------------------

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


# ------------------------------ Auth helpers ------------------------------

def _get_provided_token(request: Request) -> Optional[str]:
    # Only trust the cookie to mark a session
    cookie = request.cookies.get("st_admin_token")
    return cookie.strip() if isinstance(cookie, str) else None


def _is_authorized(request: Request) -> bool:
    req = _required_token()
    if not req:
        return False
    provided = _get_provided_token(request)
    return provided == req


def _no_store(resp: Response):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"


def _render_login(request: Request, message: Optional[str] = None, clear_cookie: bool = False) -> HTMLResponse:
    ctx = {"request": request, "message": message or ""}
    resp = templates.TemplateResponse("login.html", ctx, status_code=200)
    if clear_cookie:
        resp.delete_cookie("st_admin_token", path="/")
    _no_store(resp)
    return resp


def admin_guard_api(request: Request):
    if _is_authorized(request):
        return True
    # For API calls, we keep a JSON 401
    raise HTTPException(status_code=401, detail="Invalid or missing admin token")


# ------------------------------ Users/Roles readers ------------------------------

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

app = FastAPI(title="SuperTable Redis Admin", version="1.9.0")
templates = Jinja2Templates(directory=settings.TEMPLATES_DIR)


@app.get("/favicon.ico")
def favicon():
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
def api_user_details(user_hash: str, org: Optional[str] = Query(None), sup: Optional[str] = Query(None),
                     _=Depends(admin_guard_api)):
    org, sup = resolve_pair(org, sup)
    if not org or not sup:
        raise HTTPException(404, "Tenant not found")
    obj = read_user(org, sup, user_hash)
    if not obj:
        raise HTTPException(status_code=404, detail="User not found")
    return {"hash": user_hash, "data": obj}


@app.get("/api/role/{role_hash}")
def api_role_details(role_hash: str, org: Optional[str] = Query(None), sup: Optional[str] = Query(None),
                     _=Depends(admin_guard_api)):
    org, sup = resolve_pair(org, sup)
    if not org or not sup:
        raise HTTPException(404, "Tenant not found")
    obj = read_role(org, sup, role_hash)
    if not obj:
        raise HTTPException(status_code=404, detail="Role not found")
    return {"hash": role_hash, "data": obj}


# ------------------------------ Admin page & auth routes ------------------------------

@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_form(request: Request):
    msg = None if _required_token() else "Admin token not configured. Set SUPERTABLE_ADMIN_TOKEN in your .env and restart."
    return _render_login(request, message=msg, clear_cookie=True)


@app.post("/admin/login")
def admin_login(request: Request, token: str = Form("")):
    req = _required_token()
    if not req:
        return _render_login(request,
                             message="Admin token not configured. Set SUPERTABLE_ADMIN_TOKEN in your .env and restart.",
                             clear_cookie=True)

    # Properly validate the token
    provided_token = token.strip()
    if not provided_token:
        return _render_login(request, message="Please enter a token", clear_cookie=True)

    if provided_token != req:
        return _render_login(request, message="Invalid token", clear_cookie=True)

    resp = RedirectResponse("/admin", status_code=302)
    resp.set_cookie(
        "st_admin_token",
        req,
        httponly=True,
        samesite="lax",
        path="/",
        secure=settings.SECURE_COOKIES,
        max_age=7 * 24 * 3600
    )
    _no_store(resp)
    return resp


@app.get("/admin/logout")
def admin_logout():
    resp = RedirectResponse("/admin/login", status_code=302)
    resp.delete_cookie("st_admin_token", path="/")
    _no_store(resp)
    return resp


# Add this route after the existing routes, before the entrypoint

def _parse_dotenv(path: str) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not path:
        return env
    p = Path(path)
    if not p.exists() or not p.is_file():
        return env
    try:
        content = p.read_text(encoding="utf-8", errors="ignore")
        for line in content.splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if "=" not in s:
                continue
            k, v = s.split("=", 1)
            k = k.strip()
            v = v.strip()
            # Remove surrounding quotes if present
            if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                v = v[1:-1]
            env[k] = v
    except Exception as e:
        print(f"Error parsing .env file {path}: {e}")
    return env


def _effective_settings() -> Dict[str, str]:
    keys = ["REDIS_HOST", "REDIS_PORT", "REDIS_DB", "REDIS_PASSWORD", "SUPERTABLE_ADMIN_TOKEN", "DOTENV_PATH",
            "TEMPLATES_DIR", "SECURE_COOKIES", "HOST", "PORT", "UVICORN_RELOAD"]
    out = {}
    for k in keys:
        out[k] = os.getenv(k)
    return out


@app.get("/admin/config", response_class=HTMLResponse)
def admin_config(request: Request):
    if not _is_authorized(request):
        resp = RedirectResponse("/admin/login", status_code=302)
        _no_store(resp)
        return resp

    # Check for .env files - use more comprehensive paths
    dotenv_paths = [
        settings.DOTENV_PATH,
        ".env",
        str(Path(__file__).resolve().parent / ".env"),
        str(Path(__file__).resolve().parent.parent / ".env"),  # Add parent directory
        str(Path.cwd() / ".env"),
        str(Path.cwd().parent / ".env")  # Add parent of current directory
    ]

    # Remove duplicates while preserving order
    seen = set()
    unique_paths = []
    for path_str in dotenv_paths:
        if path_str and path_str not in seen:
            seen.add(path_str)
            unique_paths.append(path_str)

    tried = []
    dotenv_found = False
    dotenv_loaded_path = ""

    for path_str in unique_paths:
        path = Path(path_str)
        exists = path.exists() and path.is_file()
        tried.append((str(path), exists))
        if exists and not dotenv_found:
            dotenv_found = True
            dotenv_loaded_path = str(path)

    # Parse .env file and get effective settings
    dotenv_vars = _parse_dotenv(dotenv_loaded_path) if dotenv_found else {}
    effective = _effective_settings()

    all_keys = sorted(set(list(dotenv_vars.keys()) + list(effective.keys())))
    rows = [{
        "key": k,
        "env_val": dotenv_vars.get(k),
        "eff_val": effective.get(k),
        "is_sensitive": any(x in k.lower() for x in ("pass", "token", "secret", "key")),
    } for k in all_keys]

    ctx = {
        "request": request,
        "dotenv_found": dotenv_found,
        "dotenv_path": dotenv_loaded_path,
        "tried": tried,
        "rows": rows,
    }

    resp = templates.TemplateResponse("config.html", ctx)
    _no_store(resp)
    return resp

@app.get("/admin", response_class=HTMLResponse)
def admin_page(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        q: Optional[str] = Query(None),
        page: int = Query(1, ge=1),
        page_size: int = Query(25, ge=5, le=200),
):
    if not _is_authorized(request):
        # Always redirect to the login page if not authed
        resp = RedirectResponse("/admin/login", status_code=302)
        _no_store(resp)
        return resp

    provided = _get_provided_token(request) or ""

    pairs = discover_pairs()
    sel_org, sel_sup = resolve_pair(org, sup)

    tenants = [{"org": o, "sup": s, "selected": (o == sel_org and s == sel_sup)} for o, s in pairs]

    if not sel_org or not sel_sup:
        resp = templates.TemplateResponse("admin.html", {
            "request": request,
            "authorized": True,
            "token": provided,
            "tenants": tenants,
            "sel_org": sel_org,
            "sel_sup": sel_sup,
            "has_tenant": False,
        })
        _no_store(resp)
        return resp

    try:
        root = catalog.get_root(sel_org, sel_sup) or {}
    except Exception:
        root = {}
    try:
        mirrors = catalog.get_mirrors(sel_org, sel_sup) or []
    except Exception:
        mirrors = []

    listing = api_list_leaves(org=sel_org, sup=sel_sup, q=q, page=page, page_size=page_size)
    raw_items = listing["items"]
    items = [{**it, "ts_fmt": _fmt_ts(int(it.get("ts", 0)))} for it in raw_items]
    total = int(listing["total"])
    pages = (total + page_size - 1) // page_size if total else 1

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
    resp = templates.TemplateResponse("admin.html", ctx)
    _no_store(resp)
    return resp


@app.get("/", response_class=HTMLResponse)
def root_redirect():
    resp = RedirectResponse("/admin/login", status_code=302)
    _no_store(resp)
    return resp


# ------------------------------ Entrypoint ------------------------------

if __name__ == "__main__":
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    reload_flag = os.getenv("UVICORN_RELOAD", "0").lower() in ("1", "true", "yes", "on")
    uvicorn.run(app, host=host, port=port, reload=reload_flag)