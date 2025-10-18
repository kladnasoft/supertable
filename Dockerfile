# Multi-purpose image: Admin web + MCP utilities + SuperTable runtime
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    SUPERTABLE_ENV_FILE=/config/.env \
    SUPERTABLE_HOME=/data/supertable \
    STORAGE_TYPE=MINIO \
    LOCKING_BACKEND=redis \
    LOG_LEVEL=INFO

# System deps (tini for proper PID 1, curl for debug)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tini ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy metadata & install Python deps first (better layer cache)
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip && \
    pip install -r /app/requirements.txt

# Copy project sources (including your own templates/ if present)
COPY . /app

# --- Default Admin templates, created only if missing ---
# If you already keep templates/ in the repo, these lines will NOT overwrite them.
RUN mkdir -p /app/templates && \
  if [ ! -f /app/templates/login.html ]; then \
    printf '%s\n' '<!doctype html><html><head><meta charset="utf-8"><title>SuperTable Admin – Login</title><style>body{font-family:system-ui,Arial;margin:3rem}form{max-width:360px}input,button{padding:.6rem;margin:.3rem 0;width:100%}</style></head><body><h2>SuperTable Admin – Login</h2>{% if message %}<p style="color:#b00">{{ message }}</p>{% endif %}<form method="post" action="/admin/login"><label>Admin Token</label><input type="password" name="token" placeholder="Enter token" required/><button type="submit">Sign in</button></form></body></html>' \
      > /app/templates/login.html; \
  fi && \
  if [ ! -f /app/templates/admin.html ]; then \
    printf '%s\n' '<!doctype html><html><head><meta charset="utf-8"><title>SuperTable Admin</title><style>body{font-family:system-ui,Arial;margin:1.5rem}table{border-collapse:collapse;width:100%}td,th{border:1px solid #ddd;padding:.4rem}th{text-align:left;background:#f7f7f7}code{background:#f2f2f2;padding:.1rem .3rem;border-radius:.2rem}</style></head><body><header><h2>SuperTable Admin</h2><nav><a href="/admin">Home</a> • <a href="/admin/config">Config</a> • <a href="/admin/logout">Logout</a></nav><hr/></header>{% if not has_tenant %}<p>No tenants discovered yet.</p>{% endif %}<section><h3>Tenant</h3><form method="get" action="/admin"><label>Organization:</label><select name="org">{% for t in tenants %}<option value="{{ t.org }}" {% if t.selected %}selected{% endif %}>{{ t.org }}</option>{% endfor %}</select> <label>Super:</label><select name="sup">{% for t in tenants %}<option value="{{ t.sup }}" {% if t.selected %}selected{% endif %}>{{ t.sup }}</option>{% endfor %}</select> <label>Filter:</label><input type="text" name="q" value="{{ q }}"/> <button type="submit">Apply</button></form><p>Root version: <code>{{ root_version }}</code> • Root ts: {{ root_ts }} • Mirrors: {{ mirrors|join(", ") }}</p></section><section><h3>Tables ({{ total }})</h3><table><thead><tr><th>Table</th><th>Version</th><th>Updated</th><th>Path</th></tr></thead><tbody>{% for it in items %}<tr><td>{{ it.simple }}</td><td>{{ it.version }}</td><td>{{ it.ts_fmt }}</td><td><code>{{ it.path }}</code></td></tr>{% endfor %}</tbody></table><p>Page {{ page }} / {{ pages }}</p></section><section><h3>Users ({{ users|length }})</h3><table><thead><tr><th>Hash</th><th>Name</th><th>Roles</th></tr></thead><tbody>{% for u in users %}<tr><td><code>{{ u.hash }}</code></td><td>{{ u.name }}</td><td>{{ u.roles|join(", ") }}</td></tr>{% endfor %}</tbody></table></section><section><h3>Roles ({{ roles|length }})</h3><table><thead><tr><th>Hash</th><th>Name</th><th>Description</th></tr></thead><tbody>{% for r in roles %}<tr><td><code>{{ r.hash }}</code></td><td>{{ r.name }}</td><td>{{ r.description }}</td></tr>{% endfor %}</tbody></table></section></body></html>' \
      > /app/templates/admin.html; \
  fi && \
  if [ ! -f /app/templates/config.html ]; then \
    printf '%s\n' '<!doctype html><html><head><meta charset="utf-8"><title>SuperTable Admin – Config</title><style>body{font-family:system-ui,Arial;margin:1.5rem}table{border-collapse:collapse;width:100%}td,th{border:1px solid #ddd;padding:.4rem}th{background:#f7f7f7;text-align:left}code{background:#f2f2f2;padding:.1rem .3rem;border-radius:.2rem}</style></head><body><h2>SuperTable Admin – Config</h2><p>.env detected: <strong>{{ "yes" if dotenv_found else "no" }}</strong>{% if dotenv_found %} ({{ dotenv_path }}){% endif %}</p><h3>Environment</h3><table><thead><tr><th>Key</th><th>.env value</th><th>Effective</th></tr></thead><tbody>{% for r in rows %}<tr><td>{{ r.key }}</td><td>{% if r.is_sensitive and r.env_val %}<em>•••</em>{% else %}<code>{{ r.env_val }}</code>{% endif %}</td><td>{% if r.is_sensitive and r.eff_val %}<em>•••</em>{% else %}<code>{{ r.eff_val }}</code>{% endif %}</td></tr>{% endfor %}</tbody></table><p><a href="/admin">Back</a></p></body></html>' \
      > /app/templates/config.html; \
  fi

# Non-root
RUN useradd -m -u 10001 appuser && mkdir -p /data /config && chown -R appuser:appuser /app /data /config
USER appuser

# Entrypoint (config generator) + handy wrappers
COPY --chmod=0755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN printf '%s\n' '#!/usr/bin/env bash\nexec python -u /app/mcp_server.py' > /usr/local/bin/mcp-server && \
    printf '%s\n' '#!/usr/bin/env bash\nexec python -u /app/mcp_client.py "$@"' > /usr/local/bin/mcp-client && \
    chmod +x /usr/local/bin/mcp-server /usr/local/bin/mcp-client

EXPOSE 8000
ENTRYPOINT ["/usr/bin/tini","-g","--","/usr/local/bin/docker-entrypoint.sh"]
# Default runs the Admin UI; MCP is available via `mcp-server` or `mcp-client`
CMD ["uvicorn","admin:app","--host","0.0.0.0","--port","8000"]
