import os
import logging
from dataclasses import dataclass

import colorlog
from dotenv import load_dotenv, find_dotenv

# ---------- colored logging ----------
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s - %(levelname)-8s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    log_colors={'DEBUG': 'cyan','INFO': 'green','WARNING': 'yellow','ERROR': 'red','CRITICAL': 'red,bg_white'},
    style='%'
))
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger(__name__)

@dataclass
class Default:
    MAX_MEMORY_CHUNK_SIZE: int = 16 * 1024 * 1024
    MAX_OVERLAPPING_FILES: int = 100
    DEFAULT_TIMEOUT_SEC: int = 60
    DEFAULT_LOCK_DURATION_SEC: int = 30
    LOG_LEVEL: str = "INFO"
    IS_SHOW_TIMING: bool = True
    STORAGE_TYPE: str = "LOCAL"

    def update_default(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
                if k == "LOG_LEVEL":
                    self._update_log_level()

    def _update_log_level(self):
        logging.getLogger().setLevel(self.LOG_LEVEL)
        logger.debug(f"Log level changed to {self.LOG_LEVEL}")

def _parse_bool(val: str, default: bool = True) -> bool:
    if val is None:
        return default
    return val.strip().lower() in ("1","true","yes","y","on")

def _load_env(env_file: str | None, prefer_system: bool) -> str | None:
    """
    Returns the path of the .env that was loaded (or None).
    prefer_system=True  -> .env fills missing keys only (override=False)
    prefer_system=False -> .env can override system env (override=True)
    """
    # If a specific env_file path is not given, try to discover one up the tree
    path = env_file if env_file else find_dotenv(usecwd=True)
    if path and os.path.isfile(path):
        load_dotenv(path, override=not prefer_system)
        logger.debug(f".env loaded from: {path} (override={'off' if prefer_system else 'on'})")
        return path
    else:
        logger.info(".env not found (skipped). Working dir: %s", os.getcwd())
        return None

def load_defaults_from_env(env_file: str | None = None, prefer_system: bool = True) -> Default:
    _load_env(env_file, prefer_system=prefer_system)

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    if log_level not in {"DEBUG","INFO","WARNING","ERROR","CRITICAL"}:
        logger.warning(f"Invalid LOG_LEVEL={log_level!r}. Falling back to INFO.")
        log_level = "INFO"
    logging.getLogger().setLevel(log_level)

    return Default(
        MAX_MEMORY_CHUNK_SIZE=int(os.getenv("MAX_MEMORY_CHUNK_SIZE", 16 * 1024 * 1024)),
        MAX_OVERLAPPING_FILES=int(os.getenv("MAX_OVERLAPPING_FILES", 100)),
        DEFAULT_TIMEOUT_SEC=int(os.getenv("DEFAULT_TIMEOUT_SEC", 60)),
        DEFAULT_LOCK_DURATION_SEC=int(os.getenv("DEFAULT_LOCK_DURATION_SEC", 30)),
        LOG_LEVEL=log_level,
        IS_SHOW_TIMING=_parse_bool(os.getenv("IS_SHOW_TIMING", "true"), True),
        STORAGE_TYPE=os.getenv("STORAGE_TYPE", "LOCAL").upper(),
    )

# module-level default (system env wins by default)
default = load_defaults_from_env(prefer_system=True)

def refresh_defaults(env_file: str | None = None, prefer_system: bool = True) -> None:
    global default
    default = load_defaults_from_env(env_file=env_file, prefer_system=prefer_system)
    logger.info(f"Defaults refreshed. STORAGE_TYPE={default.STORAGE_TYPE}, LOG_LEVEL={default.LOG_LEVEL}")

def print_config() -> None:
    keys = [
        "STORAGE_TYPE","SUPERTABLE_HOME",
        "AWS_ACCESS_KEY_ID","AWS_SECRET_ACCESS_KEY",
        "AWS_S3_ENDPOINT_URL","AWS_S3_FORCE_PATH_STYLE",
        "REDIS_URL","LOG_LEVEL",
    ]
    logger.info("---- Effective SuperTable configuration ----")
    for k in keys:
        v = os.getenv(k)
        if k == "AWS_SECRET_ACCESS_KEY" and v:
            v = v[:4] + "****" + v[-2:]
        logger.info(f"{k} = {v}")
    logger.info(
        f"(defaults) STORAGE_TYPE={default.STORAGE_TYPE}, "
        f"LOG_LEVEL={default.LOG_LEVEL}, "
        f"IS_SHOW_TIMING={default.IS_SHOW_TIMING}, "
        f"MAX_MEMORY_CHUNK_SIZE={default.MAX_MEMORY_CHUNK_SIZE}, "
        f"MAX_OVERLAPPING_FILES={default.MAX_OVERLAPPING_FILES}"
    )
