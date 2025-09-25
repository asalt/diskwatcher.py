import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional

from diskwatcher.utils import config as config_utils
from diskwatcher.utils.logging import get_logger

logger = get_logger(__name__)

DB_PATH = config_utils.config_dir() / "diskwatcher.db"


def init_db(path=None):
    if path is None:
        path = DB_PATH
    logger.debug(f"Initializing database at {path}")

    conn = sqlite3.connect(path, timeout=30.0)
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON")
    cur.execute("PRAGMA busy_timeout = 10000")
    try:
        cur.execute("PRAGMA journal_mode = WAL")
    except sqlite3.OperationalError:
        pass
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            event_type TEXT,
            path TEXT,
            directory TEXT,
            volume_id TEXT,
            process_id TEXT
        )
    """
    )
    conn.commit()
    return conn
