import logging
import sys
from pathlib import Path

from diskwatcher.utils import config as config_utils

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOG_DIR = config_utils.config_dir()
LOG_FILE = LOG_DIR / "diskwatcher.log"


def setup_logging(level=logging.INFO):
    """Set up logging to console and file."""
    logger = logging.getLogger(__name__)

    handlers = [logging.StreamHandler(sys.stdout)]  # Console output

    log_file = LOG_FILE
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
