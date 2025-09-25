import logging
import sys
from pathlib import Path

from diskwatcher.utils import config as config_utils

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOG_DIR = config_utils.config_dir()
LOG_FILE = LOG_DIR / "diskwatcher.log"

def setup_logging(level=logging.INFO):
    """Set up logging to console and file."""
    if not LOG_DIR.exists():
        LOG_DIR.mkdir(parents=True, exist_ok=True)

    handlers = [
        logging.StreamHandler(sys.stdout),  # Console output
        logging.FileHandler(LOG_FILE, mode="a")  # File output
    ]

    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        handlers=handlers
    )

    logging.info("Logging initialized.")

def get_logger(name: str):
    """Get a logger instance."""
    return logging.getLogger(name)
