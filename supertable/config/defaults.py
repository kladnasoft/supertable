# route: supertable.config.defaults
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
