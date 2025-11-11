thonimport logging
import os
from typing import Optional

_LOGGER_CONFIGURED = False

def _configure_root_logger(level: int) -> None:
    global _LOGGER_CONFIGURED
    if _LOGGER_CONFIGURED:
        return

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(handler)

    _LOGGER_CONFIGURED = True

def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Return a configured logger instance.

    Logging level can be controlled via the FAST_CRAWLER_LOG_LEVEL env var.
    """
    level_name = os.getenv("FAST_CRAWLER_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    _configure_root_logger(level)
    return logging.getLogger(name)