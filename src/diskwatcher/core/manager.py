import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from threading import Event, Lock, Thread
from concurrent.futures import ProcessPoolExecutor, as_completed

from diskwatcher.core.watcher import DiskWatcher, DiskWatcherThread
from diskwatcher.utils.devices import get_mount_info
from diskwatcher.utils.logging import get_logger
from diskwatcher.db import init_db
from diskwatcher.db.jobs import JobHandle

logger = get_logger(__name__)


def _normalize_sqlite_path(raw: str) -> Path:
    if raw.startswith("file:"):
        trimmed = raw[5:]
        if trimmed.startswith("///"):
            trimmed = trimmed[2:]
        if "?" in trimmed:
            trimmed = trimmed.split("?", 1)[0]
        raw = trimmed
    return Path(raw)


class DiskWatcherManager:
    def __init__(
        self,
        conn: Optional[sqlite3.Connection] = None,
        *,
        polling_interval: Optional[int] = None,
        exclude_patterns: Optional[Iterable[str]] = None,
    ):
        self._owns_connection = conn is None
        self.conn = conn or init_db()
        self.conn_lock = Lock() if self.conn else None
        self.threads: List[DiskWatcherThread] = []
        self._threads_lock = Lock()
        self._running = False
        self._auto_discovery: Optional[_AutoDiscoveryThread] = None
        self._polling_interval = polling_interval
        self._exclude_patterns = list(exclude_patterns) if exclude_patterns else []

    def add_directory(self, path: Path, uuid: Optional[str] = None):
        path = path.resolve()
        with self._threads_lock:
            for existing in self.threads:
                if existing.path == path:
                    return existing

        if uuid is None:
            try:
                info = get_mount_info(path)
                uuid = (
                    info.get("volume_id")
                    or info.get("uuid")
                    or info.get("label")
                    or info.get("device")
                    or str(path)
                )
            except Exception as e:
                logger.warning(f"Could not resolve ID for {path}: {e}")
                uuid = str(path)

        watcher_thread = DiskWatcherThread(
            path,
            uuid,
            conn=self.conn,
            conn_lock=self.conn_lock,
            polling_interval=self._polling_interval,
            exclude_patterns=self._exclude_patterns,
        )
        with self._threads_lock:
            self.threads.append(watcher_thread)
        return watcher_thread

    def _database_path(self) -> Optional[Path]:
        if not self.conn:
            return None

        try:
            cursor = self.conn.execute("PRAGMA database_list")
        except sqlite3.Error:
            return None

        for name, _, path in cursor.fetchall():
            if name == "main" and path and path not in ("", ":memory:"):
                return _normalize_sqlite_path(path)

        if self._owns_connection:
            from diskwatcher.db.connection import DB_PATH as _DB_PATH

            return Path(_DB_PATH)

        return None

    def run_initial_scans(
        self,
        *,
        parallel: bool = True,
        max_workers: Optional[int] = None,
        threads: Optional[Iterable[DiskWatcherThread]] = None,
    ) -> List[Dict[str, Any]]:
        """Archive existing files for all directories, optionally in parallel."""

        target_threads: List[DiskWatcherThread]
        if threads is not None:
            target_threads = list(threads)
        else:
            target_threads = self._snapshot_threads()

        if not target_threads:
            return []

        scan_jobs: Dict[DiskWatcherThread, JobHandle] = {}
        queued_at = datetime.now(timezone.utc).isoformat()
        for thread in target_threads:
            thread.watcher.scan_stats = {
                "status": "queued",
                "queued_at": queued_at,
                "uuid": thread.uuid,
                "path": str(thread.path),
            }
            scan_jobs[thread] = JobHandle.start(
                self.conn,
                job_type="initial_scan",
                path=str(thread.path),
                volume_id=thread.uuid,
                status="queued",
                lock=self.conn_lock,
            )

        if not parallel:
            targets = ", ".join(f"{thread.path} (volume={thread.uuid})" for thread in target_threads)
            logger.info(
                "initial_scan_serial directories=%d targets=%s",
                len(target_threads),
                targets,
                extra={
                    "directories": len(target_threads),
                    "targets": [{"path": str(thread.path), "volume_id": thread.uuid} for thread in target_threads],
                },
            )
            results: List[Dict[str, Any]] = []
            for thread in target_threads:
                job_handle = scan_jobs[thread]
                job_handle.update(status="running")
                try:
                    stats = thread.watcher.archive_existing_files(job_tracker=job_handle)
                except Exception as exc:  # pragma: no cover - defensive guard
                    job_handle.fail(error=str(exc))
                    raise
                stats.setdefault("uuid", thread.uuid)
                stats.setdefault("path", str(thread.path))
                final_status = stats.get("status", "complete")
                job_handle.complete(status=final_status, progress=stats)
                results.append(stats)
            return results

        db_path = self._database_path()
        if self.conn:
            try:
                if self.conn_lock:
                    with self.conn_lock:
                        self.conn.commit()
                else:
                    self.conn.commit()
            except sqlite3.Error:
                logger.debug("initial_scan_commit_failed", exc_info=True)

        worker_path = str(db_path) if db_path else None
        if worker_path is None:
            logger.warning(
                "initial_scan_parallel_fallback reason=no_database_path",
                extra={"reason": "no_database_path"},
            )
            return self.run_initial_scans(parallel=False, threads=target_threads)

        if max_workers is None:
            cpu_hint = os.cpu_count() or len(target_threads)
            max_parallel = max(1, cpu_hint)
        else:
            max_parallel = max(1, max_workers)

        desired_workers = min(len(target_threads), max_parallel)
        targets = ", ".join(f"{thread.path} (volume={thread.uuid})" for thread in target_threads)

        logger.info(
            "initial_scan_parallel_start directories=%d workers=%d targets=%s",
            len(target_threads),
            desired_workers,
            targets,
            extra={
                "directories": len(target_threads),
                "workers": desired_workers,
                "database": worker_path,
                "targets": [{"path": str(thread.path), "volume_id": thread.uuid} for thread in target_threads],
            },
        )

        results: List[Dict[str, Any]] = []
        futures: Dict[Any, Tuple[DiskWatcherThread, JobHandle]] = {}

        with ProcessPoolExecutor(max_workers=desired_workers) as executor:
            for thread in target_threads:
                job_handle = scan_jobs[thread]
                futures[
                    executor.submit(
                        _archive_directory_in_process,
                        str(thread.path),
                        thread.uuid,
                        worker_path,
                        job_handle.job_id,
                    )
                ] = (thread, job_handle)

            for future in as_completed(futures):
                thread, job_handle = futures[future]
                try:
                    stats = future.result()
                except Exception as exc:  # pragma: no cover - defensive guard
                    logger.error(
                        "initial_scan_failed path=%s error=%s",
                        str(thread.path),
                        str(exc),
                        extra={"path": str(thread.path), "error": str(exc)},
                    )
                    failure_stats = {
                        "status": "failed",
                        "error": str(exc),
                        "failed_at": datetime.now(timezone.utc).isoformat(),
                        "uuid": thread.uuid,
                        "path": str(thread.path),
                    }
                    job_handle.fail(error=str(exc), progress=failure_stats)
                    thread.watcher.scan_stats = failure_stats
                    results.append(failure_stats)
                else:
                    stats["uuid"] = thread.uuid
                    stats["path"] = str(thread.path)
                    thread.watcher.scan_stats = stats
                    final_status = stats.get("status", "complete")
                    job_handle.complete(status=final_status, progress=stats)
                    results.append(stats)

        return results


    def start_all(self):
        self._running = True
        threads = self._snapshot_threads()
        for t in threads:
            if t.watch_job is None:
                t.set_watcher_job(
                    JobHandle.start(
                        self.conn,
                        job_type="watcher",
                        path=str(t.path),
                        volume_id=t.uuid,
                        status="starting",
                        lock=self.conn_lock,
                    )
                )
            if not t.is_alive():
                t.start()
        logger.info(f"Started {len(threads)} watcher threads")

    def stop_all(self):
        logger.info("Stopping all watchers...")
        self.disable_auto_discovery()
        threads = self._snapshot_threads()
        for t in threads:
            t.stop()
        for t in threads:
            if t.is_alive():
                t.join()
        for t in threads:
            t.clear_watcher_job(status="stopped")
        self._running = False

        if self._owns_connection and self.conn:
            logger.debug("Closing shared database connection")
            self.conn.close()

    def status(self) -> List[dict]:
        """Return current status of each thread."""
        statuses = []
        for thread in self._snapshot_threads():
            scan_stats = getattr(thread.watcher, "scan_stats", {})
            entry = {
                "path": str(thread.path),
                "uuid": thread.uuid,
                "alive": thread.is_alive(),
            }
            if scan_stats:
                entry["scan"] = scan_stats
            else:
                entry["scan"] = {"status": "pending"}
            statuses.append(entry)
        return statuses

    def current_paths(self) -> List[Path]:
        return [thread.path for thread in self._snapshot_threads()]

    @property
    def is_running(self) -> bool:
        return self._running

    def start_thread(self, thread: DiskWatcherThread) -> None:
        if thread.is_alive():
            return
        if thread.watch_job is None:
            thread.set_watcher_job(
                JobHandle.start(
                    self.conn,
                    job_type="watcher",
                    path=str(thread.path),
                    volume_id=thread.uuid,
                    status="starting",
                    lock=self.conn_lock,
                )
            )
        try:
            thread.start()
        except RuntimeError:
            logger.debug(
                "start_thread_skipped",
                extra={"path": str(thread.path)},
            )

    def remove_directory(self, path: Path) -> bool:
        resolved = path.resolve()
        with self._threads_lock:
            for idx, thread in enumerate(self.threads):
                if thread.path == resolved:
                    self.threads.pop(idx)
                    break
            else:
                return False

        logger.info(
            "auto_removed_directory path=%s",
            str(resolved),
            extra={"path": str(resolved)},
        )
        thread.stop()
        if thread.is_alive():
            thread.join()
        thread.clear_watcher_job(status="removed")
        return True

    def enable_auto_discovery(
        self,
        roots: Iterable[Path],
        *,
        scan_new: bool = True,
        max_workers: Optional[int] = None,
        interval: float = 5.0,
        start_thread: bool = True,
    ) -> None:
        def _resolve(candidate: Path) -> Path:
            try:
                return candidate.resolve()
            except FileNotFoundError:
                return candidate

        normalized: List[Path] = []
        for root in roots:
            candidate = Path(root).expanduser()
            normalized.append(_resolve(candidate))

        unique_roots = []
        seen: Set[Path] = set()
        for root in normalized:
            if root in seen:
                continue
            seen.add(root)
            unique_roots.append(root)

        if not unique_roots:
            return

        self.disable_auto_discovery()

        thread = _AutoDiscoveryThread(
            manager=self,
            roots=unique_roots,
            scan_new=scan_new,
            max_workers=max_workers,
            interval=interval,
        )

        # Prime discovery before the background loop runs so current mounts attach immediately.
        thread.scan_once()
        self._auto_discovery = thread
        if start_thread:
            thread.start()

    def disable_auto_discovery(self) -> None:
        thread = self._auto_discovery
        if not thread:
            return
        thread.stop()
        if thread.is_alive():
            thread.join()
        self._auto_discovery = None

    def start_auto_discovery_thread(self) -> None:
        thread = self._auto_discovery
        if thread and not thread.is_alive() and not thread.stopped():
            thread.start()

    def set_auto_discovery_scan_new(self, enabled: bool) -> None:
        thread = self._auto_discovery
        if thread is not None:
            thread.scan_new = enabled

    def _snapshot_threads(self) -> List[DiskWatcherThread]:
        with self._threads_lock:
            return list(self.threads)


class _AutoDiscoveryThread(Thread):
    def __init__(
        self,
        *,
        manager: "DiskWatcherManager",
        roots: Iterable[Path],
        scan_new: bool,
        max_workers: Optional[int],
        interval: float,
    ) -> None:
        super().__init__(daemon=True)
        self.manager = manager
        self.roots = tuple(Path(root) for root in roots)
        self.scan_new = scan_new
        self.max_workers = max_workers
        self.interval = max(interval, 1.0)
        self._stop_event = Event()
        self.auto_paths: Set[Path] = set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            self.scan_once()
            self._stop_event.wait(self.interval)

    def stop(self) -> None:
        self._stop_event.set()

    def stopped(self) -> bool:
        return self._stop_event.is_set()

    def scan_once(self) -> None:
        try:
            self._sync_state()
        except Exception:  # pragma: no cover - defensive guard
            logger.exception(
                "auto_discovery_scan_failed",
                extra={"roots": [str(root) for root in self.roots]},
            )

    def _sync_state(self) -> None:
        discovered = self._collect_directories()
        existing_paths = set(self.manager.current_paths())

        new_threads: List[DiskWatcherThread] = []

        for path in sorted(discovered):
            if path in existing_paths:
                continue
            thread = self.manager.add_directory(path)
            existing_paths.add(path)
            self.auto_paths.add(path)
            new_threads.append(thread)

        if new_threads:
            targets = ", ".join(f"{thread.path} (volume={thread.uuid})" for thread in new_threads)
            logger.info(
                "auto_discovery_found count=%d targets=%s",
                len(new_threads),
                targets,
                extra={
                    "roots": [str(root) for root in self.roots],
                    "paths": [str(thread.path) for thread in new_threads],
                    "targets": [
                        {"path": str(thread.path), "volume_id": thread.uuid} for thread in new_threads
                    ],
                },
            )

            if self.scan_new:
                self.manager.run_initial_scans(
                    parallel=True,
                    max_workers=self.max_workers,
                    threads=new_threads,
                )

            if self.manager.is_running:
                for thread in new_threads:
                    self.manager.start_thread(thread)

        for path in list(self.auto_paths):
            if path not in discovered:
                removed = self.manager.remove_directory(path)
                if removed:
                    self.auto_paths.discard(path)

    def _collect_directories(self) -> Set[Path]:
        discovered: Set[Path] = set()
        for root in self.roots:
            try:
                resolved_root = root.expanduser().resolve()
            except Exception:
                resolved_root = root

            if not resolved_root.exists():
                continue

            try:
                entries = list(resolved_root.iterdir())
            except OSError:
                continue

            for entry in entries:
                try:
                    if not entry.is_dir():
                        continue
                    resolved = entry.resolve()
                except OSError:
                    continue
                if not os.path.ismount(resolved):
                    continue
                discovered.add(resolved)

        return discovered


def _archive_directory_in_process(
    path: str,
    uuid: str,
    database_path: str,
    job_id: str,
) -> Dict[str, Any]:
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = init_db(path=Path(database_path), check_same_thread=False)
        watcher = DiskWatcher(path, uuid=uuid, conn=conn)
        job_handle = JobHandle.attach(conn, job_id)
        job_handle.update(status="running")
        stats = watcher.archive_existing_files(job_tracker=job_handle)
        final_status = stats.get("status", "complete")
        job_handle.complete(status=final_status, progress=stats)
        stats.setdefault("uuid", uuid)
        stats.setdefault("path", path)
        return stats
    finally:
        if conn is not None:
            conn.close()
