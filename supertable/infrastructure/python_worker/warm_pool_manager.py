# warm_pool_manager.py
#
# Warm pool + stateful notebook sessions (one container per session_id)
# - Warm pool: fast, stateless one-shot runs (execute_and_stream)
# - Stateful: execute_in_session_and_stream(session_id, code) persists variables across runs
#
# NOTE: This file fixes an indentation/truncation bug where _bg_maintenance and other methods
# were defined outside the class, causing:
#   AttributeError: 'WarmPoolManager' object has no attribute '_bg_maintenance'

from __future__ import annotations

import base64
import os
import pickle
import queue
import re
import threading
import time
from typing import Any, Dict, Set

import docker

from supertable.notebook.resource_config import ResourceConfig


class WarmPoolManager:
    """
    Manages:
      1) A warm pool of short-lived containers for stateless, fast execution.
      2) A per-session container map for "notebook-like" stateful execution.

    The stateful execution persists variables between cell runs by snapshotting/restoring
    a pickled namespace file inside the session container.
    """

    # Global active container tracking to safely support multiple pools
    _GLOBAL_ACTIVE_CONTAINER_IDS: Set[str] = set()
    _GLOBAL_LOCK = threading.Lock()

    def __init__(self, pool_size: int = 5, config: ResourceConfig = ResourceConfig()):
        self.client = docker.from_env(timeout=60)
        self.image_name = "code-runner-slim"
        self.config = config
        self.pool_size = int(pool_size)
        self.pool: "queue.Queue[Any]" = queue.Queue()

        # Track all containers currently managed by this instance
        self.active_containers: Set[Any] = set()
        self._lock = threading.Lock()

        # Stateful notebook sessions (session_id -> {"container": <docker_container>, "last_used": float})
        self._sessions: Dict[str, Dict[str, Any]] = {}
        self._sessions_lock = threading.Lock()

        # Initial cleanup of existing ghosts (best-effort)
        self.cleanup_orphans()

        # Start the background maintenance thread
        self.stop_event = threading.Event()
        self.maintenance_thread = threading.Thread(target=self._bg_maintenance, daemon=True)
        self.maintenance_thread.start()

        # Fill warm pool
        for _ in range(self.pool_size):
            try:
                self._add_container_to_pool()
            except Exception as e:
                print(f"Warm pool initialization failed: {e}")
                break

    # -----------------------
    # Background maintenance
    # -----------------------

    def _session_ttl_seconds(self) -> int:
        raw = os.getenv("SUPERTABLE_NOTEBOOK_SESSION_TTL_SECONDS", "").strip()
        if not raw:
            return 1800  # 30 minutes
        try:
            v = int(raw)
        except Exception:
            return 1800
        return v if v >= 60 else 60

    def _reap_idle_sessions(self) -> None:
        ttl = self._session_ttl_seconds()
        now = time.monotonic()
        with self._sessions_lock:
            items = list(self._sessions.items())

        for sid, rec in items:
            try:
                last = float(rec.get("last_used") or 0.0)
                if last and (now - last) > ttl:
                    self.reset_session(sid)
            except Exception:
                # never let maintenance crash
                pass

    def _bg_maintenance(self) -> None:
        """Periodically scans for leaked containers and reaps idle sessions."""
        while not self.stop_event.is_set():
            time.sleep(60)
            try:
                self.cleanup_orphans()
            except Exception:
                pass
            try:
                self._reap_idle_sessions()
            except Exception:
                pass

    def cleanup_orphans(self) -> None:
        """Force-removes any containers from the code-runner image not in our active set."""
        try:
            filters = {"ancestor": self.image_name}
            all_containers = self.client.containers.list(all=True, filters=filters)

            with self._GLOBAL_LOCK:
                active_ids = set(self._GLOBAL_ACTIVE_CONTAINER_IDS)

            for container in all_containers:
                if container.id not in active_ids:
                    try:
                        container.remove(force=True)
                    except Exception:
                        pass
        except Exception as e:
            print(f"Cleanup error: {e}")

    # -----------------------
    # Warm pool (stateless)
    # -----------------------

    def _add_container_to_pool(self) -> None:
        dns_servers = ["8.8.8.8", "8.8.4.4"] if not self.config.network_disabled else None

        container = self.client.containers.run(
            image=self.image_name,
            command="tail -f /dev/null",
            detach=True,
            mem_limit=self.config.mem_limit,
            cpu_period=self.config.cpu_period,
            cpu_quota=self.config.cpu_quota,
            network_disabled=self.config.network_disabled,
            dns=dns_servers,
            security_opt=["no-new-privileges"],
        )

        with self._lock:
            self.active_containers.add(container)

        with self._GLOBAL_LOCK:
            self._GLOBAL_ACTIVE_CONTAINER_IDS.add(container.id)

        self.pool.put(container)

    def _safe_add_to_pool(self) -> None:
        try:
            self._add_container_to_pool()
        except Exception as e:
            print(f"Failed to refill warm pool: {e}")

    def get_container(self):
        container = self.pool.get()
        threading.Thread(target=self._safe_add_to_pool, daemon=True).start()
        return container

    def execute_sync(self, code_str: str) -> str:
        """Synchronous wrapper for tests."""
        out = []
        for chunk in self.execute_and_stream(code_str):
            out.append(chunk)
        return "".join(out).strip()

    def execute_and_stream(self, code_str: str):
        """
        Stateless execution: runs `python3 -u -c <code>` in a warm container,
        then kills/removes that container (the pool refills asynchronously).
        """
        container = self.get_container()
        start_time = time.monotonic()
        total_bytes = 0

        try:
            exec_instance = container.exec_run(
                cmd=["python3", "-u", "-c", code_str],
                workdir="/home/sandbox",
                stream=True,
            )

            for line in exec_instance.output:
                if time.monotonic() - start_time > self.config.timeout:
                    yield "\n[Error: Timeout]"
                    break

                total_bytes += len(line)
                if total_bytes > self.config.max_output_len:
                    yield f"\n[Error: Output limit of {self.config.max_output_len} bytes exceeded]"
                    break

                yield line.decode("utf-8", errors="replace")

        except Exception as e:
            yield f"Error: {str(e)}"
        finally:
            self._finalize_container(container)

    def _finalize_container(self, container) -> None:
        """Ensures container is stopped, removed, and untracked."""
        try:
            container.kill()
        except Exception:
            pass

        try:
            container.remove()
        except Exception:
            pass

        with self._lock:
            try:
                self.active_containers.remove(container)
            except KeyError:
                pass

        with self._GLOBAL_LOCK:
            try:
                self._GLOBAL_ACTIVE_CONTAINER_IDS.discard(container.id)
            except Exception:
                pass

    # -----------------------
    # Stateful notebook sessions
    # -----------------------

    def _sanitize_session_id(self, session_id: str) -> str:
        s = str(session_id or "").strip()
        if not s:
            return "default"
        # Keep it filesystem-safe (inside container)
        return re.sub(r"[^a-zA-Z0-9_.-]+", "_", s)[:128] or "default"

    def _create_session_container(self):
        dns_servers = ["8.8.8.8", "8.8.4.4"] if not self.config.network_disabled else None

        container = self.client.containers.run(
            image=self.image_name,
            command="tail -f /dev/null",
            detach=True,
            mem_limit=self.config.mem_limit,
            cpu_period=self.config.cpu_period,
            cpu_quota=self.config.cpu_quota,
            network_disabled=self.config.network_disabled,
            dns=dns_servers,
            security_opt=["no-new-privileges"],
        )

        with self._lock:
            self.active_containers.add(container)

        with self._GLOBAL_LOCK:
            self._GLOBAL_ACTIVE_CONTAINER_IDS.add(container.id)

        return container

    def get_or_create_session_container(self, session_id: str):
        sid = self._sanitize_session_id(session_id)

        with self._sessions_lock:
            rec = self._sessions.get(sid)
            container = rec.get("container") if rec else None

        if container is not None:
            try:
                container.reload()
                if getattr(container, "status", "") == "running":
                    with self._sessions_lock:
                        self._sessions[sid]["last_used"] = time.monotonic()
                    return container
            except Exception:
                # fall through to recreate
                pass

        container = self._create_session_container()
        with self._sessions_lock:
            self._sessions[sid] = {"container": container, "last_used": time.monotonic()}
        return container

    def reset_session(self, session_id: str) -> None:
        """Hard reset of a notebook session (kills/removes the session container)."""
        sid = self._sanitize_session_id(session_id)

        with self._sessions_lock:
            rec = self._sessions.pop(sid, None)

        container = rec.get("container") if rec else None
        if container is None:
            return

        self._finalize_container(container)

    def execute_in_session_and_stream(self, session_id: str, code_str: str):
        """
        Stateful execution: keeps variables across runs using a per-session state file
        inside a long-lived container.

        Approach:
          - decode user code
          - load prior namespace dict from /home/sandbox/.nb_state/<session_id>.pkl
          - execute (Jupyter-like: print last expression if it's an Expr)
          - save picklable symbols back to the pickle file
        """
        container = self.get_or_create_session_container(session_id)
        sid = self._sanitize_session_id(session_id)
        start_time = time.monotonic()
        total_bytes = 0

        b64 = base64.b64encode((code_str or "").encode("utf-8")).decode("ascii")

        # NOTE: Keep this wrapper self-contained (no external deps).
        wrapper = f"""
import os, base64, pickle, traceback, ast

STATE_DIR = "/home/sandbox/.nb_state"
os.makedirs(STATE_DIR, exist_ok=True)
STATE_PATH = os.path.join(STATE_DIR, "{sid}.pkl")

# Restore prior namespace
ns = {{}}
if os.path.exists(STATE_PATH):
    try:
        with open(STATE_PATH, "rb") as f:
            prev = pickle.load(f)
        if isinstance(prev, dict):
            ns.update(prev)
    except Exception:
        pass

def _exec_like_jupyter(src: str):
    tree = ast.parse(src, mode="exec")
    if not tree.body:
        return
    last = tree.body[-1]
    if isinstance(last, ast.Expr):
        pre = ast.Module(body=tree.body[:-1], type_ignores=[])
        expr = ast.Expression(body=last.value)
        exec(compile(pre, "<notebook>", "exec"), ns, ns)
        val = eval(compile(expr, "<notebook>", "eval"), ns, ns)
        if val is not None:
            print(repr(val))
        return
    exec(compile(tree, "<notebook>", "exec"), ns, ns)

code = base64.b64decode("{b64}").decode("utf-8", errors="replace")
try:
    _exec_like_jupyter(code)
except SystemExit:
    raise
except Exception:
    traceback.print_exc()

# Persist namespace: best effort (filter non-picklables)
user_out = {{}}
for k, v in list(ns.items()):
    if isinstance(k, str) and k.startswith("_"):
        continue
    user_out[k] = v

def _is_picklable(x) -> bool:
    try:
        pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
        return True
    except Exception:
        return False

filtered = {{}}
for k, v in user_out.items():
    if _is_picklable(v):
        filtered[k] = v

with open(STATE_PATH, "wb") as f:
    pickle.dump(filtered, f, protocol=pickle.HIGHEST_PROTOCOL)
"""

        try:
            exec_instance = container.exec_run(
                cmd=["python3", "-u", "-c", wrapper],
                workdir="/home/sandbox",
                stream=True,
            )

            for line in exec_instance.output:
                if time.monotonic() - start_time > self.config.timeout:
                    yield "\n[Error: Timeout]"
                    break

                total_bytes += len(line)
                if total_bytes > self.config.max_output_len:
                    yield f"\n[Error: Output limit of {self.config.max_output_len} bytes exceeded]"
                    break

                yield line.decode("utf-8", errors="replace")

        except Exception as e:
            yield f"Error: {str(e)}"
        finally:
            with self._sessions_lock:
                rec = self._sessions.get(sid)
                if rec:
                    rec["last_used"] = time.monotonic()

    # -----------------------
    # Shutdown
    # -----------------------

    def shutdown_pool(self) -> None:
        """Stops maintenance and clears all containers (pool + sessions)."""
        self.stop_event.set()
        try:
            if self.maintenance_thread.is_alive():
                # Don't block shutdown; best-effort join
                self.maintenance_thread.join(timeout=1.0)
        except Exception:
            pass

        # Clear stateful sessions
        try:
            with self._sessions_lock:
                sess_ids = list(self._sessions.keys())
            for sid in sess_ids:
                try:
                    self.reset_session(sid)
                except Exception:
                    pass
        except Exception:
            pass

        # Clear warm pool containers
        with self._lock:
            for container in list(self.active_containers):
                try:
                    container.remove(force=True)
                    with self._GLOBAL_LOCK:
                        self._GLOBAL_ACTIVE_CONTAINER_IDS.discard(container.id)
                except Exception:
                    pass
            self.active_containers.clear()
