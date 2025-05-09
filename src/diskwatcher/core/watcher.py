import sqlite3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import threading
import time
from diskwatcher.utils.logging import get_logger
from diskwatcher.db import log_event, init_db, create_schema

from pathlib import Path

from threading import Event
from typing import Optional

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = get_logger(__name__)


class DiskWatcher(FileSystemEventHandler):
    """Watches for file system changes in a given directory."""

    def __init__(
        self,
        path: str,
        uuid: str = None,
        conn: Optional[sqlite3.Connection] = None,
        log_to_db: bool = True,
    ):
        self.path = Path(path)
        if uuid is None:
            from diskwatcher.utils.devices import get_mount_info

            possible_uuid = get_mount_info(path)
            uuid = (
                possible_uuid["uuid"]
                or possible_uuid["label"]
                or possible_uuid["device"]
            )
        self.uuid = uuid
        self.conn = conn
        self.log_to_db = log_to_db

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
        if self.conn:
            logger.info(f"Logging event to existing connection: {self.conn}")
            log_event(
                self.conn,
                event_type,
                path,
                str(self.path),
                self.uuid,
                str(os.getpid()),
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

    def archive_existing_files(self, interruptible: bool = False):
        """
        Recursively walk each watched dir and log all files as 'existing'.
        If interruptible is True, watcher.stop_event can be set to cancel.
        """
        # create a threading.Event on self if not already
        if not hasattr(self, "stop_event"):
            self.stop_event = threading.Event()
        for root, dirs, files in os.walk(self.path):
            if interruptible and self.stop_event.is_set():
                logger.info("Archival scan interrupted.")
                return
            for fname in files:
                full = Path(root) / fname
                self.log_event(
                    "existing", str(full), str(self.path), self.uuid, str(os.getpid())
                )


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
        conn=None,  # ⬅️ Optional shared SQLite connection
    ):
        super().__init__(daemon=True)
        self.path = path.resolve()
        self.uuid = uuid
        self.stop_event = threading.Event()
        self.conn = conn

        self.watcher = DiskWatcher(
            str(self.path),
            uuid=self.uuid,
            conn=self.conn,  # ⬅️ Pass shared connection
        )

    def run(self):
        try:
            self.watcher.start(stop_event=self.stop_event)
        except Exception as e:
            print(f"[{self.path}] Watcher error: {e}")

    def stop(self):
        self.stop_event.set()
