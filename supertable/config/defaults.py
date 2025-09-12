import os
import logging
import sys

from dotenv import load_dotenv
from dataclasses import dataclass

import colorlog

# Configure colors for all levels
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s - %(levelname)-8s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',  # White background with red text
    },
    secondary_log_colors={},
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
        """
        Updates fields of this Default instance in-place
        with any matching keys in kwargs.
        """
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
                if key == "LOG_LEVEL":
                    self._update_log_level()

    def _update_log_level(self):
        """Update the logging level based on the current setting"""
        logging.getLogger().setLevel(self.LOG_LEVEL)
        logger.info(f"Log level changed to {self.LOG_LEVEL}")


def load_defaults_from_env(env_file: str = ".env") -> Default:
    load_dotenv(env_file)

    # Get log level from env, default to INFO
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    # Validate log level
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if log_level not in valid_levels:
        log_level = "INFO"
        logger.warning(f"Invalid LOG_LEVEL in .env. Using default INFO. Valid levels are: {valid_levels}")

    # Set the log level immediately
    logging.getLogger().setLevel(log_level)

    return Default(
        MAX_MEMORY_CHUNK_SIZE=int(os.getenv("MAX_MEMORY_CHUNK_SIZE", 16 * 1024 * 1024)),
        MAX_OVERLAPPING_FILES=int(os.getenv("MAX_OVERLAPPING_FILES", 100)),

        DEFAULT_TIMEOUT_SEC=int(os.getenv("DEFAULT_TIMEOUT_SEC", 60)),
        DEFAULT_LOCK_DURATION_SEC=int(os.getenv("DEFAULT_LOCK_DURATION_SEC", 30)),
        LOG_LEVEL=log_level,
        IS_SHOW_TIMING=(os.getenv("IS_SHOW_TIMING", "True").lower() == "true"),
        STORAGE_TYPE=os.getenv("STORAGE_TYPE", "LOCAL").upper(),
    )


default = load_defaults_from_env()
