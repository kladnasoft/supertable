# supertable/storage/synapse_storage.py
# -----------------------------------------------------------------------------
# One-call activation for Synapse / ABFSS:
#   - LocalStorage I/O (json/bytes/exists/ls/size/delete/parquet) via fsspec+adlfs
#   - Polars .write_parquet -> fsspec+pyarrow (no recursion)
#   - DataReader -> caches Parquet locally, runs on a pristine DuckDB connection
#   - MonitoringLogger/MonitoringReader -> ABFSS-safe merge/read
#   - Locking -> directory-based exclusive locks (no 'xb'); shared reads via storage
#
# Usage:
#   from supertable.storage.synapse_storage import activate_synapse, silence_azure_http_logs
#   activate_synapse(home="abfss://<acct>@<fs>.dfs.core.windows.net/supertable")
#   silence_azure_http_logs(logging.INFO)
# -----------------------------------------------------------------------------

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import time
import traceback
from typing import Any, Callable, Dict, Iterable, List, Optional

__all__ = ["activate_synapse", "silence_azure_http_logs"]

_PATCHED = False  # idempotency guard


# ============================ Public: logging helper ============================

def silence_azure_http_logs(level: int = __import__("logging").WARNING) -> None:
    """
    Suppress Azure SDK/adlfs/fsspec HTTP chatter (INFO/DEBUG) without muting your app logs.
    """
    import logging
    noisy_loggers: Iterable[str] = (
        "azure",
        "azure.core.pipeline.policies.http_logging_policy",
        "azure.core.pipeline.transport",
        "azure.storage",
        "azure.storage.blob",
        "adlfs",
        "fsspec",
        "urllib3",
        "aiohttp.access", "aiohttp.client",
        "chardet.charsetprober",
    )
    for name in noisy_loggers:
        lg = logging.getLogger(name)
        lg.setLevel(level)
        lg.propagate = False
        if not lg.handlers:
            lg.addHandler(logging.NullHandler())

    # Extra safety: disable HttpLoggingPolicy header/query whitelists
    try:
        from azure.core.pipeline.policies import HttpLoggingPolicy  # type: ignore
        try:
            HttpLoggingPolicy.DEFAULT_HEADERS_WHITELIST.clear()          # type: ignore[attr-defined]
            HttpLoggingPolicy.DEFAULT_QUERY_PARAMETERS_WHITELIST.clear()  # type: ignore[attr-defined]
        except Exception:
            HttpLoggingPolicy.SENSITIVE_HEADERS = {"*"}              # type: ignore[attr-defined]
            HttpLoggingPolicy.SENSITIVE_QUERY_PARAMETERS = {"*"}     # type: ignore[attr-defined]
    except Exception:
        pass


# ============================ Public: activation ============================

def activate_synapse(
    home: str,
    *,
    cache_dir: Optional[str] = None,
    duckdb_memory_limit: str = "2GB",
) -> None:
    """
    Patch Supertable to run reliably on Azure Synapse (ABFSS).

    - home: ABFSS root for Supertable (e.g., 'abfss://.../supertable').
    - cache_dir: local directory for DuckDB parquet cache (default: /tmp/supertable_duck_cache).
    - duckdb_memory_limit: PRAGMA memory_limit value (e.g., '2GB').

    Idempotent: safe to call more than once.
    """
    global _PATCHED
    if _PATCHED:
        return

    if not home or not isinstance(home, str):
        raise ValueError("activate_synapse(home=...) is required (ABFSS path).")

    # ---------- Configure env ----------
    os.environ["SUPERTABLE_HOME"] = home
    SUPERTABLE_HOME = home
    if cache_dir:
        os.environ["SUPERTABLE_LOCAL_CACHE"] = cache_dir
    LOCAL_CACHE_DIR = os.path.join(tempfile.gettempdir(), "supertable_duck_cache")
    LOCAL_CACHE_DIR = os.environ.get("SUPERTABLE_LOCAL_CACHE", LOCAL_CACHE_DIR)
    os.makedirs(LOCAL_CACHE_DIR, exist_ok=True)

    # ---------- Logger ----------
    try:
        from supertable.config.defaults import logger  # type: ignore
    except Exception:
        import logging
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger("supertable.synapse")

    # ---------- Deps ----------
    try:
        import fsspec  # noqa: F401
    except Exception as exc:
        raise RuntimeError("Install: %pip install fsspec adlfs pyarrow polars duckdb") from exc
    try:
        import adlfs  # noqa: F401
    except Exception:
        pass
    try:
        import pyarrow as pa  # noqa: F401
        import pyarrow.parquet as pq  # noqa: F401
    except Exception as exc:
        raise RuntimeError("Install: %pip install pyarrow") from exc
    try:
        import polars as _pl  # noqa: F401
    except Exception as exc:
        raise RuntimeError("Install: %pip install polars") from exc
    import importlib
    import pyarrow as pa
    import pyarrow.parquet as pq
    import polars as pl

    # ---------- Helpers ----------
    def _is_cloud_uri(p: str) -> bool:
        return isinstance(p, str) and p.startswith(("abfs://", "abfss://", "s3://", "gs://", "wasbs://", "az://"))

    def _join_uri(base: str, rel: str) -> str:
        if not rel or rel in (".", "./"):
            return base
        if _is_cloud_uri(rel):
            return rel
        return f"{base.rstrip('/')}/{rel.lstrip('/')}"

    def _to_full_uri(path: str) -> str:
        if _is_cloud_uri(path):
            return path
        return _join_uri(SUPERTABLE_HOME, path)

    def _fs_and_path(target: str):
        import fsspec as _fs
        fs, _, _ = _fs.get_fs_token_paths(target)
        return fs, target

    def _file_size(path: str) -> int:
        try:
            return os.path.getsize(path)
        except Exception:
            return -1

    def _ensure_local_copy_verbose(path_like: str) -> str:
        """Copy abfss:// (or relative) parquet to LOCAL cache and return local path."""
        import hashlib
        target = _to_full_uri(path_like)
        os.makedirs(LOCAL_CACHE_DIR, exist_ok=True)
        local_name = hashlib.sha1(target.encode("utf-8")).hexdigest() + ".parquet"
        local_path = os.path.join(LOCAL_CACHE_DIR, local_name)

        if os.path.exists(local_path):
            logger.debug(f"[reader.cache] HIT  → {path_like} → {local_path} ({_file_size(local_path)} bytes)")
            return local_path

        fs, norm = _fs_and_path(target)
        logger.debug(f"[reader.cache] MISS → opening {norm} via {fs.__class__.__name__}")
        try:
            with fs.open(norm, "rb") as src, open(local_path, "wb") as dst:
                while True:
                    chunk = src.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)
            logger.debug(f"[reader.cache] WROTE {_file_size(local_path)} bytes → {local_path}")
        except Exception:
            logger.error(f"[reader.cache] FAILED copying {norm} → {local_path}")
            logger.error(traceback.format_exc())
            raise
        return local_path

    def _get_pristine_duckdb():
        """Ensure we use an unwrapped duckdb module (defensive reload)."""
        mod = sys.modules.get("duckdb")
        if mod is None:
            import duckdb as _duckdb  # type: ignore
            mod = _duckdb
        connect = getattr(mod, "connect", None)
        if not callable(connect) or getattr(connect, "__module__", "") != "duckdb":
            logger.debug("[reader.patch] Detected monkeypatched duckdb.connect → reloading duckdb")
            mod = importlib.reload(mod)
        return mod

    # ===================== Patch 1: LocalStorage (fsspec/adlfs) =====================

    import supertable.storage.local_storage as _ls  # type: ignore

    _ORIG_read_json = getattr(_ls.LocalStorage, "read_json", None)
    _ORIG_write_json = getattr(_ls.LocalStorage, "write_json", None)
    _ORIG_read_bytes = getattr(_ls.LocalStorage, "read_bytes", None)
    _ORIG_write_bytes = getattr(_ls.LocalStorage, "write_bytes", None)
    _ORIG_exists = getattr(_ls.LocalStorage, "exists", None)
    _ORIG_mkdirs = getattr(_ls.LocalStorage, "mkdirs", None) or getattr(_ls.LocalStorage, "makedirs", None)
    _ORIG_size = getattr(_ls.LocalStorage, "size", None)
    _ORIG_delete = getattr(_ls.LocalStorage, "delete", None)
    _ORIG_list_files = getattr(_ls.LocalStorage, "list_files", None)
    _ORIG_get_dir = getattr(_ls.LocalStorage, "get_directory_structure", None)
    _ORIG_write_parquet = getattr(_ls.LocalStorage, "write_parquet", None)
    _ORIG_read_parquet = getattr(_ls.LocalStorage, "read_parquet", None)

    def _ls_read_json(self, path: str, retries: int = 3, backoff: float = 0.05) -> Any:
        target = _to_full_uri(path) if SUPERTABLE_HOME else path
        fs, norm = _fs_and_path(target)
        last_err: Optional[Exception] = None
        for _ in range(max(1, retries)):
            try:
                with fs.open(norm, "rb") as f:
                    return json.load(io.TextIOWrapper(f, encoding="utf-8"))
            except FileNotFoundError:
                raise
            except Exception as e:
                last_err = e
                time.sleep(backoff)
        assert last_err is not None
        raise last_err

    def _ls_write_json(self, path: str, obj: Any, retries: int = 3, backoff: float = 0.05) -> None:
        target = _to_full_uri(path) if SUPERTABLE_HOME else path
        fs, norm = _fs_and_path(target)
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        last_err: Optional[Exception] = None
        for _ in range(max(1, retries)):
            try:
                parent = norm.rsplit("/", 1)[0]
                try:
                    fs.mkdirs(parent, exist_ok=True)
                except Exception:
                    pass
                with fs.open(norm, "wb") as f:
                    f.write(data)
                return
            except Exception as e:
                last_err = e
                time.sleep(backoff)
        assert last_err is not None
        raise last_err

    def _ls_read_bytes(self, path: str) -> bytes:
        fs, norm = _fs_and_path(_to_full_uri(path))
        with fs.open(norm, "rb") as f:
            return f.read()

    def _ls_write_bytes(self, path: str, data: bytes) -> None:
        fs, norm = _fs_and_path(_to_full_uri(path))
        parent = norm.rsplit("/", 1)[0]
        try:
            fs.mkdirs(parent, exist_ok=True)
        except Exception:
            pass
        with fs.open(norm, "wb") as f:
            f.write(data)

    def _ls_exists(self, path: str) -> bool:
        fs, norm = _fs_and_path(_to_full_uri(path))
        try:
            return fs.exists(norm)
        except Exception:
            try:
                fs.info(norm)
                return True
            except Exception:
                return False

    def _ls_mkdirs(self, path: str) -> None:
        fs, norm = _fs_and_path(_to_full_uri(path))
        try:
            fs.mkdirs(norm, exist_ok=True)
        except Exception:
            pass

    def _ls_size(self, path: str) -> int:
        fs, norm = _fs_and_path(_to_full_uri(path))
        if hasattr(fs, "size"):
            return int(fs.size(norm))
        info = fs.info(norm)
        size_val = info.get("size")
        if size_val is None:
            with fs.open(norm, "rb") as f:
                return len(f.read())
        return int(size_val)

    def _ls_delete(self, path: str) -> None:
        fs, norm = _fs_and_path(_to_full_uri(path))
        fs.rm(norm, recursive=True)

    def _ls_list_files(self, path: str, pattern: str = "*") -> List[str]:
        fs, norm = _fs_and_path(_to_full_uri(path))
        try:
            return fs.glob(f"{norm.rstrip('/')}/{pattern}")
        except Exception:
            try:
                return [entry["name"] for entry in fs.ls(norm)]
            except Exception:
                return []

    def _ls_get_directory_structure(self, path: str) -> dict:
        fs, norm = _fs_and_path(_to_full_uri(path))
        result: Dict[str, Any] = {}
        def _recurse(prefix: str, node: dict):
            try:
                entries = fs.ls(prefix, detail=True)
            except FileNotFoundError:
                return
            for e in entries:
                name = e.get("name") or e.get("Key") or ""
                key = name.rstrip("/").split("/")[-1]
                if e.get("type") == "directory" or name.endswith("/"):
                    node[key] = {}
                    _recurse(name, node[key])
                else:
                    node[key] = None
        _recurse(norm, result)
        return result

    def _ls_write_parquet(self, table: pa.Table, path: str) -> None:
        fs, norm = _fs_and_path(_to_full_uri(path))
        parent = norm.rsplit("/", 1)[0]
        try:
            fs.mkdirs(parent, exist_ok=True)
        except Exception:
            pass
        with fs.open(norm, "wb") as f:
            pq.write_table(table, f)

    def _ls_read_parquet(self, path: str) -> pa.Table:
        fs, norm = _fs_and_path(_to_full_uri(path))
        with fs.open(norm, "rb") as f:
            return pq.read_table(f)

    # Apply LocalStorage patches
    setattr(_ls.LocalStorage, "read_json", _ls_read_json)
    setattr(_ls.LocalStorage, "write_json", _ls_write_json)
    setattr(_ls.LocalStorage, "read_bytes", _ls_read_bytes)
    setattr(_ls.LocalStorage, "write_bytes", _ls_write_bytes)
    setattr(_ls.LocalStorage, "exists", _ls_exists)
    setattr(_ls.LocalStorage, "mkdirs", _ls_mkdirs)
    setattr(_ls.LocalStorage, "makedirs", _ls_mkdirs)  # be flexible
    setattr(_ls.LocalStorage, "size", _ls_size)
    setattr(_ls.LocalStorage, "delete", _ls_delete)
    setattr(_ls.LocalStorage, "list_files", _ls_list_files)
    setattr(_ls.LocalStorage, "get_directory_structure", _ls_get_directory_structure)
    setattr(_ls.LocalStorage, "write_parquet", _ls_write_parquet)
    setattr(_ls.LocalStorage, "read_parquet", _ls_read_parquet)

    # ===================== Patch 2: Locking (directory-based, no 'xb') =====================

    import supertable.locking.locking as _locking_mod  # type: ignore
    _Locking = getattr(_locking_mod, "Locking")
    _ORIG_lock_resources = getattr(_Locking, "lock_resources", None)
    _ORIG_unlock_resources = getattr(_Locking, "unlock_resources", None)
    _ORIG_lock_shared_and_read = getattr(_Locking, "lock_shared_and_read", None)
    _ORIG_lock_exclusive_and_write = getattr(_Locking, "lock_exclusive_and_write", None)
    _ORIG_self_lock = getattr(_Locking, "self_lock", None)
    _ORIG_self_unlock = getattr(_Locking, "self_unlock", None)

    def _lockfile_for(path: str) -> str:
        # We'll use a *directory* for the lock; the path we return is the lock directory
        return f"{path}.lock"

    def _lock_dir_path(lock_path: str) -> str:
        # Ensure we always operate on a directory path (no trailing slash needed for fsspec)
        return lock_path if not lock_path.endswith(".json") else lock_path[:-5]  # strip .json if present

    def _acquire_dir_lock(fs, lock_dir: str, ttl_seconds: int = 300, retries: int = 300, sleep: float = 0.2) -> None:
        """
        Acquire an exclusive lock by atomically creating a *directory*.
        If it already exists, look for 'lease' file with expiry; if stale, remove dir and retry.
        """
        lease_path = f"{lock_dir}/lease"
        for _ in range(max(1, retries)):
            try:
                # Attempt to create the directory atomically
                fs.mkdirs(lock_dir, exist_ok=False)
                # Write expiry to a small file inside the dir
                with fs.open(lease_path, "wb") as f:
                    f.write(str(time.time() + ttl_seconds).encode("utf-8"))
                return
            except FileExistsError:
                # Directory exists: check expiry
                try:
                    with fs.open(lease_path, "rb") as f:
                        data = f.read(64)
                    expiry = float(data.decode("utf-8")) if data else 0.0
                except Exception:
                    expiry = 0.0
                if expiry and time.time() > expiry:
                    # Stale lock → remove the whole directory and try again
                    try:
                        fs.rm(lock_dir, recursive=True)
                        continue
                    except Exception:
                        pass
                time.sleep(sleep)
        raise TimeoutError(f"Could not acquire lock directory: {lock_dir}")

    def _release_dir_lock(fs, lock_dir: str) -> None:
        try:
            fs.rm(lock_dir, recursive=True)
        except FileNotFoundError:
            pass
        except Exception:
            pass

    # --- resource lock (noop on cloud; per-file locks suffice) ---
    def _lk_lock_resources(self, resource_id: str) -> None:
        return

    def _lk_unlock_resources(self, resource_id: str) -> None:
        return

    # --- shared read: just read JSON via storage ---
    def _lk_lock_shared_and_read(self, path: str):
        from supertable.storage.local_storage import LocalStorage  # type: ignore
        storage = LocalStorage()
        return storage.read_json(path)

    # --- exclusive write: directory lock around writer() -> write_json ---
    def _lk_lock_exclusive_and_write(self, path: str, writer: Callable[[], Dict[str, Any]]):
        target = _to_full_uri(path)
        fs, norm = _fs_and_path(target)
        lock_dir = _lock_dir_path(_lockfile_for(norm))
        from supertable.storage.local_storage import LocalStorage  # type: ignore
        storage = LocalStorage()

        # Ensure parent exists, then acquire dir lock
        try:
            fs.mkdirs(lock_dir.rsplit("/", 1)[0], exist_ok=True)
        except Exception:
            pass
        _acquire_dir_lock(fs, lock_dir)
        try:
            obj = writer()
            storage.write_json(path, obj)
            return obj
        finally:
            _release_dir_lock(fs, lock_dir)

    # --- self_lock/self_unlock used by SuperTable.update_with_lock ---
    def _lk_self_lock(self, timeout_seconds: int = 60, lock_duration_seconds: int = 30) -> bool:
        """
        Acquire a process-wide ABFSS directory lock for this Locking identity.
        Prefer a lock in `working_dir/.lock` if available; otherwise
        fall back to SUPERTABLE_HOME/locks/{identity}.lock
        """
        working_dir = getattr(self, "working_dir", None) or getattr(self, "working_dir_path", None)
        identity = getattr(self, "identity", "global")

        if working_dir:
            lock_rel = os.path.join(working_dir, ".lock")
        else:
            lock_rel = os.path.join("locks", f"{identity}.lock")

        lock_target = _to_full_uri(lock_rel)
        fs, norm = _fs_and_path(lock_target)

        # Ensure parent dir exists (important for the fallback path)
        try:
            parent = norm.rsplit("/", 1)[0]
            fs.mkdirs(parent, exist_ok=True)
        except Exception:
            pass

        lock_dir = norm  # already a directory path by construction
        setattr(self, "_cloud_lock_state", (fs, lock_dir))
        retries = max(1, int((timeout_seconds / 0.2)))
        log_where = f"working_dir={working_dir!r}" if working_dir else f"fallback='locks/{identity}.lock'"
        try:
            from supertable.config.defaults import logger as _slog  # type: ignore
            _slog.debug(f"[cloud-lock] trying {log_where} → {lock_dir}")
        except Exception:
            logger.debug(f"[cloud-lock] trying {log_where} → {lock_dir}")

        _acquire_dir_lock(fs, lock_dir, ttl_seconds=int(lock_duration_seconds), retries=retries, sleep=0.2)
        try:
            from supertable.config.defaults import logger as _slog  # type: ignore
            _slog.debug(f"[cloud-lock] acquired: {lock_dir}")
        except Exception:
            logger.debug(f"[cloud-lock] acquired: {lock_dir}")
        return True

    def _lk_self_unlock(self) -> None:
        st = getattr(self, "_cloud_lock_state", None)
        if st:
            fs, lock_dir = st
            _release_dir_lock(fs, lock_dir)
            try:
                from supertable.config.defaults import logger as _slog  # type: ignore
                _slog.debug(f"[cloud-lock] released: {lock_dir}")
            except Exception:
                logger.debug(f"[cloud-lock] released: {lock_dir}")
            setattr(self, "_cloud_lock_state", None)

    setattr(_Locking, "lock_resources", _lk_lock_resources)
    setattr(_Locking, "unlock_resources", _lk_unlock_resources)
    setattr(_Locking, "lock_shared_and_read", _lk_lock_shared_and_read)
    setattr(_Locking, "lock_exclusive_and_write", _lk_lock_exclusive_and_write)
    setattr(_Locking, "self_lock", _lk_self_lock)
    setattr(_Locking, "self_unlock", _lk_self_unlock)

    # ===================== Patch 3: Polars write_parquet (fsspec+pyarrow) =====================

    _PL_ORIG_WRITE_PARQUET = getattr(pl.DataFrame, "write_parquet", None)

    def _already_wrapped(func: Callable[..., Any]) -> bool:
        return getattr(func, "__supertable_cloud_wrapped__", False) is True

    def _cloudsafe_pl_write_parquet(self, *args, **kwargs):
        file_arg: Optional[Any] = None
        if "file" in kwargs:
            file_arg = kwargs["file"]
        elif args:
            file_arg = args[0]

        if not isinstance(file_arg, str):
            if _PL_ORIG_WRITE_PARQUET and not _already_wrapped(_PL_ORIG_WRITE_PARQUET):
                return _PL_ORIG_WRITE_PARQUET(self, *args, **kwargs)
            table = self.to_arrow()
            sink = file_arg
            compression = kwargs.get("compression", "zstd")
            compression_level = kwargs.get("compression_level", None)
            pq.write_table(table, sink, compression=compression, compression_level=compression_level)
            return

        target = _to_full_uri(file_arg)
        compression = kwargs.get("compression", "zstd")
        compression_level = kwargs.get("compression_level", None)

        table = self.to_arrow()
        fs, norm = _fs_and_path(target)
        parent = norm.rsplit("/", 1)[0]
        try:
            fs.mkdirs(parent, exist_ok=True)
        except Exception:
            pass
        with fs.open(norm, "wb") as f:
            pq.write_table(table, f, compression=compression, compression_level=compression_level)
        return None

    if _PL_ORIG_WRITE_PARQUET is not None and not _already_wrapped(_PL_ORIG_WRITE_PARQUET):
        setattr(_cloudsafe_pl_write_parquet, "__supertable_cloud_wrapped__", True)
        setattr(pl.DataFrame, "write_parquet", _cloudsafe_pl_write_parquet)

    # ===================== Patch 4: DataReader (cache parquet + pristine DuckDB) =====================

    import supertable.data_reader as _dr  # type: ignore
    _ORIG_execute_with_duckdb = getattr(_dr.DataReader, "execute_with_duckdb", None)

    def _patched_execute_with_duckdb(self, parquet_files, query_manager):
        logger.debug(self._lp(f"[reader.patch] activate execute_with_duckdb, files={len(parquet_files)}"))
        try:
            local_files: List[str] = []
            for p in parquet_files:
                lp = _ensure_local_copy_verbose(p)
                local_files.append(lp)
            for src, dst in zip(parquet_files, local_files):
                logger.debug(self._lp(f"[reader.paths] {src}  ->  {dst}  ({_file_size(dst)} bytes)"))

            _duckdb = _get_pristine_duckdb()
            logger.debug(self._lp(f"[duckdb] using module={_duckdb!r}, connect={getattr(_duckdb, 'connect', None)!r}"))
            con = _duckdb.connect()
            try:
                self.timer.capture_and_reset_timing("CONNECTING")
                pragmas = [
                    f"PRAGMA memory_limit='{duckdb_memory_limit}';",
                    f"PRAGMA temp_directory='{query_manager.temp_dir}';",
                    "PRAGMA enable_profiling='json';",
                    f"PRAGMA profile_output = '{query_manager.query_plan_path}';",
                    "PRAGMA default_collation='nocase';",
                ]
                for p in pragmas:
                    logger.debug(self._lp(f"[duckdb] {p.strip()}"))
                    con.execute(p)

                parquet_files_str = ", ".join(f"'{f}'" for f in local_files)
                if self.parser.columns_csv == "*":
                    safe_columns_csv = "*"
                else:
                    def _q(c: str) -> str:
                        c = c.strip()
                        if c == "*":
                            return "*"
                        if all(ch.isalnum() or ch == "_" for ch in c):
                            return c
                        return '"' + c.replace('"', '""') + '"'
                    safe_columns_csv = ", ".join(_q(c) for c in self.parser.columns_list)

                create_table = f"""
CREATE TABLE {self.parser.reflection_table}
AS
SELECT {safe_columns_csv}
FROM parquet_scan([{parquet_files_str}], union_by_name=TRUE, HIVE_PARTITIONING=TRUE);
""".strip()
                logger.debug(self._lp("[duckdb] CREATE TABLE SQL ↓"))
                logger.debug(self._lp(create_table))
                con.execute(create_table)

                create_view = f"""
CREATE VIEW {self.parser.rbac_view}
AS
{self.parser.view_definition}
""".strip()
                logger.debug(self._lp("[duckdb] CREATE VIEW SQL ↓"))
                logger.debug(self._lp(create_view))
                con.execute(create_view)

                self.timer.capture_and_reset_timing("CREATING_REFLECTION")
                logger.debug(self._lp(f"[duckdb] Executing final query: {self.parser.executing_query}"))
                result = con.execute(query=self.parser.executing_query).fetchdf()
                logger.debug(self._lp(f"[duckdb] result: rows={result.shape[0]}, cols={result.shape[1]}"))
                return result
            finally:
                try:
                    con.close()
                except Exception:
                    pass
        except Exception:
            logger.error(self._lp("[reader.patch] Unhandled exception:"))
            logger.error(self._lp(traceback.format_exc()))
            raise

    if _ORIG_execute_with_duckdb is not None:
        setattr(_dr.DataReader, "execute_with_duckdb", _patched_execute_with_duckdb)

    # ===================== Patch 5: MonitoringLogger (merge via storage) =====================

    import supertable.monitoring_logger as _ml  # type: ignore
    _ORIG_ML_write_parquet_file = getattr(_ml.MonitoringLogger, "_write_parquet_file", None)

    def _patched_ml_write_parquet_file(self, data: List[Dict[str, Any]], existing_path: Optional[str] = None) -> Dict[str, Any]:
        import polars as pl  # local import
        if not data:
            return {"file": existing_path or "", "file_size": 0, "rows": 0, "columns": 0, "stats": {}}

        logger.debug(f"[monitoring.logger] incoming batch size={len(data)} existing_path={existing_path}")
        data = [self._ensure_execution_time(record) for record in data]
        df = pl.from_dicts(data)

        if existing_path and self.storage.exists(existing_path):
            try:
                logger.debug(f"[monitoring.logger] merging into existing file: {existing_path}")
                existing_table = self.storage.read_parquet(existing_path)
                existing_df = pl.from_arrow(existing_table)
                df = pl.concat([existing_df, df], how="vertical_relaxed")
                self.storage.delete(existing_path)
            except Exception as e:
                logger.warning(f"Warning: Failed to merge with existing file {existing_path}: {str(e)}")

        new_filename = self._generate_filename("data.parquet")
        new_path = os.path.join(self.data_dir, new_filename)

        table = df.to_arrow()
        logger.debug(f"[monitoring.logger] writing parquet rows={len(df)} cols={len(df.columns)} → {new_path}")
        self.storage.write_parquet(table, new_path)

        resource = {
            "file": new_path,
            "file_size": self.storage.size(new_path),
            "rows": len(df),
            "columns": len(df.columns),
            "stats": self._calculate_stats(df)
        }
        logger.debug(f"[monitoring.logger] wrote parquet resource={resource}")
        return resource

    if _ORIG_ML_write_parquet_file is not None:
        setattr(_ml.MonitoringLogger, "_write_parquet_file", _patched_ml_write_parquet_file)

    # ===================== Patch 6: MonitoringReader (cache parquet + pristine DuckDB) =====================

    import supertable.monitoring_reader as _mr  # type: ignore
    _ORIG_MR_read = getattr(_mr.MonitoringReader, "read", None)

    def _patched_mr_read(self, from_ts_ms: Optional[int] = None, to_ts_ms: Optional[int] = None, limit: int = 1000):
        import pandas as pd
        from datetime import datetime, timezone, timedelta

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1_000)
        if to_ts_ms is None:
            to_ts_ms = now_ms
        if from_ts_ms is None:
            from_ts_ms = to_ts_ms - int(timedelta(days=1).total_seconds() * 1_000)
        if from_ts_ms > to_ts_ms:
            raise ValueError(f"from_ts_ms ({from_ts_ms}) must be <= to_ts_ms ({to_ts_ms})")

        snapshot = self._load_current_snapshot()
        parquet_files = self._collect_parquet_files(snapshot, from_ts_ms, to_ts_ms)
        if not parquet_files:
            logger.debug("[monitoring.reader] No parquet files match time window.")
            return pd.DataFrame()

        local_files: List[str] = []
        for p in parquet_files:
            lp = _ensure_local_copy_verbose(p)
            local_files.append(lp)
            logger.debug(f"[monitoring.reader.paths] {p} -> {lp} ({_file_size(lp)} bytes)")

        _duckdb = _get_pristine_duckdb()
        con = _duckdb.connect()
        con.execute(f"PRAGMA memory_limit='{duckdb_memory_limit}';")
        con.execute(f"PRAGMA temp_directory='{self.temp_dir}';")
        con.execute("PRAGMA default_collation='nocase';")

        files_sql_array = "[" + ", ".join(f"'{f}'" for f in local_files) + "]"
        query = (
            "SELECT *\n"
            f"FROM parquet_scan({files_sql_array}, union_by_name=TRUE, HIVE_PARTITIONING=TRUE)\n"
            f"WHERE execution_time BETWEEN {from_ts_ms} AND {to_ts_ms}\n"
            "ORDER BY execution_time DESC\n"
            f"LIMIT {limit}"
        )
        logger.debug("[monitoring.reader] Executing Query:\n%s", query)

        try:
            df = con.execute(query).fetchdf()
        finally:
            try:
                con.close()
            except Exception:
                pass

        logger.debug("[monitoring.reader] Result shape: %s", df.shape)
        return df

    if _ORIG_MR_read is not None:
        setattr(_mr.MonitoringReader, "read", _patched_mr_read)

    # ---------- Final log ----------
    logger.info("Supertable Synapse/ABFSS patches activated.")
    logger.info("  SUPERTABLE_HOME = %s", SUPERTABLE_HOME)
    logger.info("  LOCAL_CACHE_DIR = %s", LOCAL_CACHE_DIR)
    logger.info("  DuckDB memory_limit = %s", duckdb_memory_limit)

    _PATCHED = True


def read_cloud_dataset_as_arrow(
    input_path: str,
    file_type: str,
    *,
    recursive: bool = True,
    add_source_columns: bool = True,
) -> "pa.Table":
    """
    Read CSV / JSON / Parquet from ABFSS (or local) folder/file and return a pyarrow.Table.

    - input_path: ABFSS URI or path relative to SUPERTABLE_HOME.
                  Can be a file OR a directory. Globs are supported.
    - file_type : 'csv' | 'json' | 'parquet'
    - recursive: if input_path is a directory, search **/*.ext when True,
                 or just *.ext when False. If input_path contains a glob, it's used as-is.
    - add_source_columns: adds 'source_path' and 'source_file' columns (like your Spark helper)

    Returns: pyarrow.Table
    """
    import logging
    import os
    import fsspec
    import pyarrow as pa
    import pyarrow.parquet as pq

    logger = logging.getLogger("supertable.synapse.read")

    # --- tiny helpers (mirror our patch logic) ---
    def _is_cloud_uri(p: str) -> bool:
        return isinstance(p, str) and p.startswith(("abfs://", "abfss://", "s3://", "gs://", "wasbs://", "az://"))

    def _join_uri(base: str, rel: str) -> str:
        if not rel or rel in (".", "./"):
            return base
        if _is_cloud_uri(rel):
            return rel
        return f"{base.rstrip('/')}/{rel.lstrip('/')}"

    def _to_full_uri(path: str) -> str:
        home = os.environ.get("SUPERTABLE_HOME")
        if _is_cloud_uri(path) or not home:
            return path
        return _join_uri(home, path)

    def _fs_and_path(target: str):
        fs, _, _ = fsspec.get_fs_token_paths(target)
        return fs, target

    # --- resolve and enumerate files ---
    file_type = (file_type or "").strip().lower()
    if file_type not in {"csv", "json", "parquet"}:
        raise ValueError(f"file_type must be one of 'csv', 'json', 'parquet' (got {file_type!r})")

    target = _to_full_uri(input_path)
    fs, norm = _fs_and_path(target)

    def _is_glob(p: str) -> bool:
        return any(ch in p for ch in ("*", "?", "["))

    files: list[str] = []
    try:
        if _is_glob(norm):
            files = fs.glob(norm) or []
        else:
            info = fs.info(norm)  # raises if missing
            typ = info.get("type") or ("directory" if norm.endswith("/") else "file")
            if typ == "file":
                files = [norm]
            else:
                ext = {"csv": "csv", "json": "json", "parquet": "parquet"}[file_type]
                pat = "**/*" if recursive else "*"
                # recursive first, then fallback to top-level if nothing found
                primary = f"{norm.rstrip('/')}/{pat}.{ext}"
                files = fs.glob(primary) or []
                if not files and recursive:
                    fallback = f"{norm.rstrip('/')}/*.{ext}"
                    logger.debug(f"[read_cloud_dataset_as_arrow] no matches for {primary}; trying {fallback}")
                    files = fs.glob(fallback) or []
    except FileNotFoundError:
        raise FileNotFoundError(f"Path does not exist: {norm!r}")

    if not files:
        raise FileNotFoundError(f"No {file_type.upper()} files found under: {norm!r}")

    files = sorted(files)
    logger.debug(f"[read_cloud_dataset_as_arrow] matched {len(files)} file(s) for {file_type}: first={files[0]!r}")

    # --- readers per type ---
    tables: list[pa.Table] = []

    if file_type == "parquet":
        for fp in files:
            with fs.open(fp, "rb") as f:
                t = pq.read_table(f)
            if add_source_columns:
                n = t.num_rows
                if n:
                    sp = pa.array([fp] * n, type=pa.string())
                    sf = pa.array([fp.rstrip('/').split('/')[-1]] * n, type=pa.string())
                    t = t.append_column("source_path", sp).append_column("source_file", sf)
            tables.append(t)

    elif file_type == "csv":
        import pyarrow.csv as pacsv
        # Default behavior: header row is used as column names (autogenerate_column_names=False)
        read_opts = pacsv.ReadOptions(autogenerate_column_names=False, use_threads=True)
        parse_opts = pacsv.ParseOptions(delimiter=",")
        conv_opts = pacsv.ConvertOptions(strings_can_be_null=True)
        for fp in files:
            with fs.open(fp, "rb") as f:
                t = pacsv.read_csv(f, read_options=read_opts, parse_options=parse_opts, convert_options=conv_opts)
            if add_source_columns:
                n = t.num_rows
                if n:
                    sp = pa.array([fp] * n, type=pa.string())
                    sf = pa.array([fp.rstrip('/').split('/')[-1]] * n, type=pa.string())
                    t = t.append_column("source_path", sp).append_column("source_file", sf)
            tables.append(t)

    else:  # json
        import pyarrow.json as pajson
        # Assumes JSON Lines (one object per line). For large, this streams efficiently.
        ro = pajson.ReadOptions(use_threads=True)  # default block size is fine
        for fp in files:
            with fs.open(fp, "rb") as f:
                t = pajson.read_json(f, read_options=ro)
            if add_source_columns:
                n = t.num_rows
                if n:
                    sp = pa.array([fp] * n, type=pa.string())
                    sf = pa.array([fp.rstrip('/').split('/')[-1]] * n, type=pa.string())
                    t = t.append_column("source_path", sp).append_column("source_file", sf)
            tables.append(t)

    # --- unify schema + concat ---
    if len(tables) == 1:
        return tables[0]
    return pa.concat_tables(tables, promote=True)
