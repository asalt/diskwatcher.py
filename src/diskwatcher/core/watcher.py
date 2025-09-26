import sqlite3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, Lock
from typing import Any, Optional

from diskwatcher.db import init_db, log_event
from diskwatcher.utils.devices import get_mount_info
from diskwatcher.utils.logging import get_logger

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = get_logger(__name__)

MOUNT_METADATA_INITIAL_REFRESH_SECONDS = 300
MOUNT_METADATA_MAX_REFRESH_SECONDS = 3600


def _metadata_complete(metadata: Optional[dict[str, Any]]) -> bool:
    if not metadata:
        return False
    lsblk = metadata.get("lsblk")
    if isinstance(lsblk, dict):
        for key in ("UUID", "PTUUID", "PARTUUID", "SERIAL", "WWN"):
            if lsblk.get(key):
                return True
    return False


class DiskWatcher(FileSystemEventHandler):
    """Watches for file system changes in a given directory."""

    def __init__(
        self,
        path: str,
        uuid: str = None,
        conn: Optional[sqlite3.Connection] = None,
        conn_lock: Optional[Lock] = None,
        log_to_db: bool = True,
    ):
        self.path = Path(path)
        self.conn = conn
        self.conn_lock = conn_lock
        self.log_to_db = log_to_db
        self.scan_stats: dict[str, Any] = {}

        self.uuid = uuid or str(self.path)
        self._mount_metadata: Optional[dict[str, Any]] = None
        self._mount_metadata_refreshed_at: float = 0.0
        self._mount_metadata_interval: float = MOUNT_METADATA_INITIAL_REFRESH_SECONDS
        self._next_mount_metadata_refresh: float = 0.0

        initial_metadata = self._refresh_mount_metadata(force=True)
        if uuid is None and initial_metadata:
            self.uuid = (
                initial_metadata.get("volume_id")
                or initial_metadata.get("uuid")
                or initial_metadata.get("label")
                or initial_metadata.get("device")
                or str(self.path)
            )

    def on_modified(self, event):
        logger.info(f"File modified: {event.src_path}")
        if self.log_to_db:
            self.log_event("modified", event.src_path)

    def on_created(self, event):
        logger.info(f"File created: {event.src_path}")
        if self.log_to_db:
            self.log_event("created", event.src_path)

    def on_deleted(self, event):
        logger.info(f"File deleted: {event.src_path}")
        if self.log_to_db:
            self.log_event("deleted", event.src_path)

    def log_event(self, event_type: str, path: str):
        mount_metadata = self._refresh_mount_metadata()
        metadata_payload = dict(mount_metadata) if mount_metadata else None
        if metadata_payload is not None:
            metadata_payload.setdefault("source", "watcher")
        if self.conn:
            logger.debug("Logging event to shared connection", extra={"volume_id": self.uuid})
            if self.conn_lock:
                with self.conn_lock:
                    log_event(
                        self.conn,
                        event_type,
                        path,
                        str(self.path),
                        self.uuid,
                        str(os.getpid()),
                        mount_metadata=metadata_payload,
                    )
            else:
                log_event(
                    self.conn,
                    event_type,
                    path,
                    str(self.path),
                    self.uuid,
                    str(os.getpid()),
                    mount_metadata=metadata_payload,
                )
        else:
            logger.info(f"Logging event to new connection")
            with init_db() as conn:
                log_event(
                    conn,
                    event_type,
                    path,
                    str(self.path),
                    self.uuid,
                    str(os.getpid()),
                    mount_metadata=metadata_payload,
                )

    def start(self, recursive=True, run_once=False, stop_event: Optional[Event] = None):

        if stop_event is not None and not isinstance(stop_event, Event):
            raise TypeError("stop_event must be a threading.Event or None")

        observer = Observer()
        observer.schedule(self, str(self.path), recursive=recursive)

        logger.info(f"Watching {self.uuid} : {self.path}...")
        observer.start()

        try:
            while True:
                time.sleep(1)
                if run_once or (stop_event and stop_event.is_set()):
                    break
        finally:
            observer.stop()
            observer.join()

    def _refresh_mount_metadata(self, force: bool = False) -> Optional[dict[str, Any]]:
        now = time.monotonic()

        if self._mount_metadata is not None and _metadata_complete(self._mount_metadata):
            return self._mount_metadata

        if not force and self._mount_metadata is not None and now < self._next_mount_metadata_refresh:
            return self._mount_metadata

        try:
            info = get_mount_info(str(self.path))
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.debug(
                "mount_metadata_refresh_failed",
                extra={"path": str(self.path), "error": str(exc)},
            )
            return self._schedule_next_mount_refresh(now)

        if not isinstance(info, dict):  # pragma: no cover - defensive guard
            return self._schedule_next_mount_refresh(now)

        metadata = dict(info)
        metadata.setdefault("mount_point", metadata.get("mount_point") or str(self.path))
        metadata.setdefault("identity_refreshed_at", datetime.now(timezone.utc).isoformat())

        self._mount_metadata = metadata
        self._mount_metadata_refreshed_at = now

        if _metadata_complete(metadata):
            self._next_mount_metadata_refresh = float("inf")
        else:
            self._schedule_next_mount_refresh(now)

        return self._mount_metadata

    def _schedule_next_mount_refresh(self, now: float) -> Optional[dict[str, Any]]:
        interval = max(self._mount_metadata_interval, MOUNT_METADATA_INITIAL_REFRESH_SECONDS)
        self._next_mount_metadata_refresh = now + interval
        self._mount_metadata_interval = min(interval * 2, MOUNT_METADATA_MAX_REFRESH_SECONDS)
        return self._mount_metadata

    def archive_existing_files(self, interruptible: bool = False):
        """
        Recursively walk each watched dir and log all files as 'existing'.
        If interruptible is True, watcher.stop_event can be set to cancel.
        """
        if not hasattr(self, "stop_event"):
            self.stop_event = threading.Event()

        started_at = time.time()
        files_scanned = 0
        directories_seen = 0
        self.scan_stats = {
            "status": "running",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "files_scanned": 0,
            "directories_seen": 0,
        }

        logger.info(
            "initial_scan_start",
            extra={
                "volume_id": self.uuid,
                "root": str(self.path),
                "started_at": self.scan_stats["started_at"],
            },
        )

        for root, dirs, files in os.walk(self.path):
            if interruptible and self.stop_event.is_set():
                logger.info(
                    "initial_scan_interrupted",
                    extra={
                        "volume_id": self.uuid,
                        "root": str(self.path),
                        "files_scanned": files_scanned,
                        "directories_seen": directories_seen,
                        "elapsed_seconds": round(time.time() - started_at, 2),
                    },
                )
                self.scan_stats.update(
                    {
                        "status": "interrupted",
                        "files_scanned": files_scanned,
                        "directories_seen": directories_seen,
                        "elapsed_seconds": round(time.time() - started_at, 2),
                    }
                )
                return
            directories_seen += 1
            for fname in files:
                full = Path(root) / fname
                self.log_event("existing", str(full))
                files_scanned += 1
                if files_scanned % 500 == 0:
                    self.scan_stats.update(
                        {
                            "status": "running",
                            "files_scanned": files_scanned,
                            "directories_seen": directories_seen,
                        }
                    )
                    logger.debug(
                        "initial_scan_progress",
                        extra={
                            "volume_id": self.uuid,
                            "files_scanned": files_scanned,
                            "directories_seen": directories_seen,
                        },
                    )

        elapsed = time.time() - started_at
        logger.info(
            "initial_scan_complete",
            extra={
                "volume_id": self.uuid,
                "root": str(self.path),
                "files_scanned": files_scanned,
                "directories_seen": directories_seen,
                "elapsed_seconds": round(elapsed, 2),
            },
        )

        self.scan_stats = {
            "status": "complete",
            "files_scanned": files_scanned,
            "directories_seen": directories_seen,
            "elapsed_seconds": elapsed,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }


# class DiskWatcherThread(threading.Thread):
#     def __init__(self, path: Path, uuid: Optional[str] = None):
#         super().__init__(daemon=True)
#         self.path = path.resolve()
#         self.uuid = uuid
#         self.stop_event = threading.Event()
#         self.watcher = DiskWatcher(str(self.path), uuid=self.uuid)

#     def run(self):
#         try:
#             self.watcher.start(stop_event=self.stop_event)
#         except Exception as e:
#             print(f"[{self.path}] Watcher error: {e}")

#     def stop(self):
#         self.stop_event.set()


class DiskWatcherThread(threading.Thread):
    def __init__(
        self,
        path: Path,
        uuid: Optional[str] = None,
        conn: Optional[sqlite3.Connection] = None,  # ⬅️ Optional shared SQLite connection
        conn_lock: Optional[Lock] = None,
    ):
        super().__init__(daemon=True)
        self.path = path.resolve()
        self.uuid = uuid
        self.stop_event = threading.Event()
        self.conn = conn
        self.conn_lock = conn_lock

        self.watcher = DiskWatcher(
            str(self.path),
            uuid=self.uuid,
            conn=self.conn,  # ⬅️ Pass shared connection
            conn_lock=self.conn_lock,
        )
        self.uuid = self.watcher.uuid

    def run(self):
        try:
            self.watcher.start(stop_event=self.stop_event)
        except Exception as e:
            logger.exception("Watcher error", extra={"path": str(self.path)})

    def stop(self):
        self.stop_event.set()
