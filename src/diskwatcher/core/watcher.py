import errno
import fnmatch
import sqlite3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, Lock
from typing import Any, Optional, Iterable

from diskwatcher.db import init_db, log_event
from diskwatcher.db.jobs import JobHandle
from diskwatcher.utils.devices import get_mount_info
from diskwatcher.utils.logging import get_logger

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = get_logger(__name__)

MOUNT_METADATA_INITIAL_REFRESH_SECONDS = 300
MOUNT_METADATA_MAX_REFRESH_SECONDS = 3600

# Design notes (watch scaling, intentionally deferred):
# A) Use non-recursive observers plus our own os.walk to apply exclude_patterns before scheduling per-directory watches.
# B) On Linux/inotify, subclass watchdog's InotifyEmitter to skip blacklisted dirs (e.g. __pycache__, .git, tmp) when adding watches.
# We currently rely on recursive observers and a polling fallback when ENOSPC is hit instead of optimizing the watch set.


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
        polling_interval: Optional[int] = None,
        exclude_patterns: Optional[Iterable[str]] = None,
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
        self.polling_interval = float(polling_interval) if polling_interval is not None else 30.0
        self.exclude_patterns = list(exclude_patterns) if exclude_patterns else []

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
        if self._is_excluded(path):
            return
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

    def _is_excluded(self, path: str) -> bool:
        if not self.exclude_patterns:
            return False
        path_str = str(path)
        for pattern in self.exclude_patterns:
            if fnmatch.fnmatch(path_str, pattern):
                return True
        return False

    def start(
        self,
        recursive=True,
        run_once=False,
        stop_event: Optional[Event] = None,
        job_tracker: Optional[JobHandle] = None,
    ):

        if stop_event is not None and not isinstance(stop_event, Event):
            raise TypeError("stop_event must be a threading.Event or None")

        observer = None
        try:
            observer = Observer()
            observer.schedule(self, str(self.path), recursive=recursive)
            logger.info(
                "watcher_started path=%s volume_id=%s backend=%s",
                str(self.path),
                self.uuid,
                "inotify",
                extra={"volume_id": self.uuid, "path": str(self.path), "backend": "inotify"},
            )
            observer.start()
        except OSError as exc:
            # Fall back to a polling observer when the inotify watch limit
            # is exhausted on the host. This avoids hard failures on large
            # directory trees at the cost of extra I/O.
            if exc.errno == errno.ENOSPC:
                logger.warning(
                    "watcher_inotify_limit_reached",
                    extra={"path": str(self.path), "error": str(exc)},
                )
                try:
                    from watchdog.observers.polling import PollingObserver
                except Exception:  # pragma: no cover - defensive guard
                    raise

                timeout = self.polling_interval if self.polling_interval and self.polling_interval > 0 else 30.0
                observer = PollingObserver(timeout=timeout)
                observer.schedule(self, str(self.path), recursive=recursive)
                logger.info(
                    "watcher_started path=%s volume_id=%s backend=%s",
                    str(self.path),
                    self.uuid,
                    "polling",
                    extra={"volume_id": self.uuid, "path": str(self.path), "backend": "polling"},
                )
                observer.start()
            else:
                raise

        if job_tracker:
            job_tracker.update(status="running", progress={"path": str(self.path)})

        try:
            while True:
                time.sleep(1)
                if job_tracker:
                    job_tracker.heartbeat()
                if run_once or (stop_event and stop_event.is_set()):
                    break
        finally:
            if observer is not None:
                observer.stop()
                observer.join()
            if job_tracker:
                job_tracker.update(status="stopping")

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

    def archive_existing_files(
        self,
        interruptible: bool = False,
        job_tracker: Optional[JobHandle] = None,
    ):
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
            "uuid": self.uuid,
            "path": str(self.path),
        }

        if job_tracker:
            job_tracker.update(status="running", progress=dict(self.scan_stats))

        logger.info(
            "initial_scan_start root=%s volume_id=%s",
            str(self.path),
            self.uuid,
            extra={
                "volume_id": self.uuid,
                "root": str(self.path),
                "started_at": self.scan_stats["started_at"],
            },
        )

        for root, dirs, files in os.walk(self.path):
            if interruptible and self.stop_event.is_set():
                logger.info(
                    "initial_scan_interrupted root=%s volume_id=%s files=%d dirs=%d",
                    str(self.path),
                    self.uuid,
                    files_scanned,
                    directories_seen,
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
                if job_tracker:
                    job_tracker.update(
                        status="interrupted",
                        progress=dict(self.scan_stats),
                    )
                return dict(self.scan_stats)
            # Skip excluded directories from traversal.
            dirs[:] = [
                d for d in dirs if not self._is_excluded(Path(root) / d)
            ]

            if self._is_excluded(root):
                continue

            directories_seen += 1
            for fname in files:
                full = Path(root) / fname
                if self._is_excluded(full):
                    continue
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
                    if job_tracker:
                        job_tracker.heartbeat(progress=dict(self.scan_stats))

        elapsed = time.time() - started_at
        logger.info(
            "initial_scan_complete root=%s volume_id=%s files=%d dirs=%d elapsed=%.2fs",
            str(self.path),
            self.uuid,
            files_scanned,
            directories_seen,
            round(elapsed, 2),
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
            "uuid": self.uuid,
            "path": str(self.path),
        }

        if job_tracker:
            job_tracker.heartbeat(progress=dict(self.scan_stats))
        return dict(self.scan_stats)

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
        conn: Optional[sqlite3.Connection] = None,  # <- Optional shared SQLite connection
        conn_lock: Optional[Lock] = None,
        polling_interval: Optional[int] = None,
        exclude_patterns: Optional[Iterable[str]] = None,
    ):
        super().__init__(daemon=True)
        self.path = path.resolve()
        self.uuid = uuid
        self.stop_event = threading.Event()
        self.conn = conn
        self.conn_lock = conn_lock
        self.scan_job: Optional[JobHandle] = None
        self.watch_job: Optional[JobHandle] = None

        self.watcher = DiskWatcher(
            str(self.path),
            uuid=self.uuid,
            conn=self.conn,  # <- Pass shared connection
            conn_lock=self.conn_lock,
            polling_interval=polling_interval,
            exclude_patterns=exclude_patterns,
        )
        self.uuid = self.watcher.uuid

    def run(self):
        if self.watch_job:
            self.watch_job.update(status="running")
        try:
            self.watcher.start(stop_event=self.stop_event, job_tracker=self.watch_job)
        except Exception as e:
            logger.exception("Watcher error", extra={"path": str(self.path)})
            if self.watch_job:
                self.watch_job.fail(error=str(e))
        else:
            if self.watch_job:
                self.watch_job.complete(status="stopped")

    def stop(self):
        self.stop_event.set()

    def set_watcher_job(self, job: JobHandle) -> None:
        self.watch_job = job

    def clear_watcher_job(self, status: str = "stopped") -> None:
        if self.watch_job:
            if status not in {"stopped", "complete"}:
                self.watch_job.update(status=status)
            self.watch_job.complete(status=status)
            self.watch_job = None
