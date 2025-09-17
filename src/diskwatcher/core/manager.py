import sqlite3
from pathlib import Path
from typing import List, Optional
from threading import Lock
from diskwatcher.core.watcher import DiskWatcherThread
from diskwatcher.utils.devices import get_mount_info
from diskwatcher.utils.logging import get_logger
from diskwatcher.db import init_db

logger = get_logger(__name__)


class DiskWatcherManager:
    def __init__(self, conn: Optional[sqlite3.Connection] = None):
        self._owns_connection = conn is None
        self.conn = conn or init_db()
        self.conn_lock = Lock() if self.conn else None
        self.threads: List[DiskWatcherThread] = []

    def add_directory(self, path: Path, uuid: Optional[str] = None):
        path = path.resolve()
        if uuid is None:
            try:
                info = get_mount_info(path)
                uuid = info["uuid"] or info["label"] or info["device"]
            except Exception as e:
                logger.warning(f"Could not resolve ID for {path}: {e}")
                uuid = str(path)

        watcher_thread = DiskWatcherThread(
            path,
            uuid,
            conn=self.conn,
            conn_lock=self.conn_lock,
        )
        self.threads.append(watcher_thread)

    def start_all(self):
        for t in self.threads:
            t.start()
        logger.info(f"Started {len(self.threads)} watcher threads")

    def stop_all(self):
        logger.info("Stopping all watchers...")
        for t in self.threads:
            t.stop()
        for t in self.threads:
            t.join()

        if self._owns_connection and self.conn:
            logger.debug("Closing shared database connection")
            self.conn.close()

    def status(self) -> List[dict]:
        """Return current status of each thread."""
        return [
            {"path": str(t.path), "uuid": t.uuid, "alive": t.is_alive()}
            for t in self.threads
        ]
