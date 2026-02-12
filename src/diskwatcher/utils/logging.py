import logging
import sys
from pathlib import Path
from typing import Optional

from diskwatcher.utils import config as config_utils

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOG_DIR = config_utils.config_dir()
LOG_FILE = LOG_DIR / "diskwatcher.log"
_ACTIVE_LOG_DIR: Optional[Path] = None
_ACTIVE_LOG_FILE: Optional[Path] = None


def active_log_dir() -> Optional[Path]:
    """Return the directory used for file logging (if enabled)."""

    return _ACTIVE_LOG_DIR


def active_log_file() -> Optional[Path]:
    """Return the file used for file logging (if enabled)."""

    return _ACTIVE_LOG_FILE


def setup_logging(level=logging.INFO):
    """Set up logging to console and file."""

    global _ACTIVE_LOG_DIR, _ACTIVE_LOG_FILE

    logger = logging.getLogger(__name__)

    handlers = [logging.StreamHandler(sys.stdout)]  # Console output

    log_file = LOG_FILE
    log_dir = LOG_DIR
    file_handler = None

    try:
        if not LOG_DIR.exists():
            LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, mode="a")
    except OSError as exc:
        # Fall back to a local, repo-friendly directory if the default
        # config location is not writable (common in sandboxes).
        fallback_dir = Path.cwd() / ".diskwatcher_logs"
        try:
            fallback_dir.mkdir(parents=True, exist_ok=True)
            log_dir = fallback_dir
            log_file = fallback_dir / "diskwatcher.log"
            file_handler = logging.FileHandler(log_file, mode="a")
            logger.warning(
                "log_file_fallback primary=%s fallback=%s error=%s",
                str(LOG_FILE),
                str(log_file),
                str(exc),
            )
        except OSError as fallback_exc:
            logger.warning(
                "log_file_disabled primary=%s fallback_dir=%s error=%s",
                str(LOG_FILE),
                str(fallback_dir),
                str(fallback_exc),
            )
            file_handler = None

    if file_handler is not None:
        handlers.append(file_handler)
        _ACTIVE_LOG_DIR = log_dir
        _ACTIVE_LOG_FILE = log_file
    else:
        _ACTIVE_LOG_DIR = None
        _ACTIVE_LOG_FILE = None

    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        handlers=handlers,
    )

    if file_handler is not None:
        logger.info("Logging initialized (log_file=%s)", str(log_file))
    else:
        logger.info("Logging initialized (no file log; console only)")


def get_logger(name: str):
    """Get a logger instance."""
    return logging.getLogger(name)
