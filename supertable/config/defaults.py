# route: supertable.config.defaults
import os
import logging
from dataclasses import dataclass

import colorlog

from supertable.config.settings import settings

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

_VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})

@dataclass(slots=True)
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


def load_defaults_from_env(env_file: str | None = None, prefer_system: bool = True) -> Default:
    log_level = settings.SUPERTABLE_LOG_LEVEL
    if log_level not in _VALID_LOG_LEVELS:
        logger.warning(f"Invalid LOG_LEVEL={log_level!r}. Falling back to INFO.")
        log_level = "INFO"
    logging.getLogger().setLevel(log_level)

    return Default(
        MAX_MEMORY_CHUNK_SIZE=settings.MAX_MEMORY_CHUNK_SIZE,
        MAX_OVERLAPPING_FILES=settings.MAX_OVERLAPPING_FILES,
        DEFAULT_TIMEOUT_SEC=settings.DEFAULT_TIMEOUT_SEC,
        DEFAULT_LOCK_DURATION_SEC=settings.DEFAULT_LOCK_DURATION_SEC,
        LOG_LEVEL=log_level,
        IS_SHOW_TIMING=settings.IS_SHOW_TIMING,
        STORAGE_TYPE=settings.STORAGE_TYPE,
    )

# module-level default (backward compatibility)
default = load_defaults_from_env(prefer_system=True)

def refresh_defaults(env_file: str | None = None, prefer_system: bool = True) -> None:
    global default
    default = load_defaults_from_env(env_file=env_file, prefer_system=prefer_system)
    logger.info(f"Defaults refreshed. STORAGE_TYPE={default.STORAGE_TYPE}, LOG_LEVEL={default.LOG_LEVEL}")

def print_config() -> None:
    _MASKED_KEYS = frozenset({"STORAGE_SECRET_KEY", "STORAGE_ACCESS_KEY", "AZURE_STORAGE_KEY", "AZURE_SAS_TOKEN"})
    keys = [
        "STORAGE_TYPE", "SUPERTABLE_HOME",
        "STORAGE_BUCKET", "STORAGE_REGION",
        "STORAGE_ENDPOINT_URL", "STORAGE_ACCESS_KEY", "STORAGE_SECRET_KEY",
        "STORAGE_FORCE_PATH_STYLE", "STORAGE_SESSION_TOKEN",
        "SUPERTABLE_DUCKDB_USE_HTTPFS", "SUPERTABLE_PREFIX",
        "REDIS_URL", "LOG_LEVEL",
    ]
    logger.info("---- Effective SuperTable configuration ----")
    for k in keys:
        v = getattr(settings, k, None)
        if v is None or v == "":
            continue
        v = str(v)
        if k in _MASKED_KEYS:
            v = "****" + v[-4:] if len(v) > 4 else "****"
        logger.info(f"{k} = {v}")
    logger.info(
        f"(defaults) STORAGE_TYPE={default.STORAGE_TYPE}, "
        f"LOG_LEVEL={default.LOG_LEVEL}, "
        f"IS_SHOW_TIMING={default.IS_SHOW_TIMING}, "
        f"MAX_MEMORY_CHUNK_SIZE={default.MAX_MEMORY_CHUNK_SIZE}, "
        f"MAX_OVERLAPPING_FILES={default.MAX_OVERLAPPING_FILES}"
    )
