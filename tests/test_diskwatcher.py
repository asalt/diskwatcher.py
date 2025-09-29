# tests/test_diskwatcher.py
import os
import sqlite3
import pytest
import logging
import threading
import time
import concurrent.futures
import shutil
from pathlib import Path

import diskwatcher.db.connection as db_connection
from diskwatcher.core.manager import DiskWatcherManager, _AutoDiscoveryThread
from diskwatcher.core.watcher import DiskWatcher, DiskWatcherThread
from watchdog.events import FileCreatedEvent, FileModifiedEvent, FileDeletedEvent

from diskwatcher.db import create_schema, query_events, init_db


@pytest.fixture
def temp_db(tmp_path):
    db_path = tmp_path / "events.db"
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    create_schema(conn)
    yield conn
    conn.close()


@pytest.mark.parametrize("log_to_db", [True, False])
def test_on_created(caplog, temp_db, log_to_db):
    watcher = DiskWatcher("/tmp", log_to_db=log_to_db, conn=temp_db)
    event = FileCreatedEvent("/tmp/test.txt")

    with caplog.at_level(logging.INFO):
        watcher.on_created(event)

    assert "File created: /tmp/test.txt" in caplog.text

    if log_to_db:
        # Check if the event was logged to the database
        cursor = temp_db.cursor()
        cursor.execute("SELECT * FROM events WHERE path = '/tmp/test.txt'")
        rows = (
            cursor.fetchall()
        )  # (id, timestamp, event_type, path, directory, volume_id, process_id)
        # print(rows)
        assert len(rows) == 1
        assert rows[0][2] == "created"
        assert rows[0][3] == "/tmp/test.txt"
        pid = os.getpid()
        assert rows[0][6] == str(pid)


@pytest.mark.parametrize("log_to_db", [True, False])
def test_on_modified(caplog, temp_db, log_to_db):
    watcher = DiskWatcher("/tmp", log_to_db=log_to_db, conn=temp_db)
    event = FileModifiedEvent("/tmp/test.txt")

    with caplog.at_level(logging.INFO):
        watcher.on_modified(event)

    assert "File modified: /tmp/test.txt" in caplog.text

    if log_to_db:
        # Check if the event was logged to the database
        cursor = temp_db.cursor()
        cursor.execute("SELECT * FROM events WHERE path = '/tmp/test.txt'")
        rows = cursor.fetchall()
        # (id, timestamp, event_type, path, directory, volume_id, process_id)
        assert len(rows) == 1
        assert rows[0][2] == "modified"
        assert rows[0][3] == "/tmp/test.txt"
        pid = os.getpid()
        assert rows[0][6] == str(pid)


@pytest.mark.parametrize("log_to_db", [True, False])
def test_on_deleted(caplog, temp_db, log_to_db):
    watcher = DiskWatcher("/tmp", log_to_db=log_to_db, conn=temp_db)
    event = FileDeletedEvent("/tmp/test.txt")

    with caplog.at_level(logging.INFO):
        watcher.on_deleted(event)

    assert "File deleted: /tmp/test.txt" in caplog.text

    if log_to_db:
        # Check if the event was logged to the database
        cursor = temp_db.cursor()
        cursor.execute("SELECT * FROM events WHERE path = '/tmp/test.txt'")
        rows = cursor.fetchall()
        # (id, timestamp, event_type, path, directory, volume_id, process_id)
        assert len(rows) == 1
        assert rows[0][2] == "deleted"
        assert rows[0][3] == "/tmp/test.txt"
        pid = os.getpid()
        assert rows[0][6] == str(pid)


def test_file_create_triggers_event_manualthread(tmp_path, caplog):
    # watcher = DiskWatcher(str(tmp_path), log_to_db=False)
    watcher = DiskWatcher(str(tmp_path), log_to_db=False)
    stop_event = threading.Event()

    thread = threading.Thread(
        target=watcher.start, kwargs={"stop_event": stop_event}, daemon=True
    )
    thread.start()

    time.sleep(0.3)
    test_file = tmp_path / "testfile.txt"
    test_file.write_text("hi")

    time.sleep(0.3)
    stop_event.set()
    thread.join()

    assert "File created" in caplog.text


def test_file_create_triggers_event(tmp_path, temp_db):
    test_dir = tmp_path / "watched"
    test_dir.mkdir()

    watcher = DiskWatcherThread(path=test_dir, conn=temp_db)
    watcher.start()
    time.sleep(0.3)  # give observer time to initialize

    file_path = test_dir / "test.txt"
    # file_path.write_text("hello world")
    with file_path.open("w") as f:
        f.write("hello world")
        f.flush()
        os.fsync(f.fileno())

    time.sleep(0.3)  # let the event be processed

    watcher.stop()
    watcher.join()

    events = query_events(temp_db, limit=10)
    for event in events:
        # print(event) # do not need to force eval here with fsync call
        assert event["event_type"] in ("created", "modified")
        # assert event["path"] == str(file_path)


def test_file_delete_triggers_event(tmp_path, temp_db):
    test_dir = tmp_path / "watched"
    test_dir.mkdir()

    file_path = test_dir / "file.txt"
    file_path.write_text("bye")

    watcher = DiskWatcherThread(path=test_dir, conn=temp_db)
    watcher.start()
    time.sleep(0.1)

    file_path.unlink()  # delete it

    time.sleep(0.3)
    watcher.stop()
    watcher.join()

    events = query_events(temp_db, limit=10)
    for event in events:
        list(event)  # force evaluation
        assert event["event_type"] in ("deleted", "modified")
        # assert event["path"] == str(file_path)


def test_archive_existing_files_logs_existing_event(tmp_path, temp_db):
    watched_dir = tmp_path / "watched"
    watched_dir.mkdir()
    existing_file = watched_dir / "already_there.txt"
    existing_file.write_text("hi")

    watcher = DiskWatcher(str(watched_dir), conn=temp_db)
    watcher.archive_existing_files()

    events = query_events(temp_db, limit=5)
    assert any(event["event_type"] == "existing" for event in events)


def test_archive_existing_files_updates_scan_stats(tmp_path, temp_db):
    watched_dir = tmp_path / "watched"
    nested = watched_dir / "nested"
    nested.mkdir(parents=True)
    for idx in range(3):
        (watched_dir / f"file_{idx}.txt").write_text("content")
    (nested / "inner.txt").write_text("inner")

    watcher = DiskWatcher(str(watched_dir), conn=temp_db)
    watcher.archive_existing_files()

    stats = watcher.scan_stats
    assert stats["status"] == "complete"
    assert stats["files_scanned"] >= 4
    assert stats["directories_seen"] >= 2
    assert stats["elapsed_seconds"] >= 0


def test_archive_existing_files_resets_stats_on_rerun(tmp_path, temp_db):
    watched_dir = tmp_path / "watched"
    watched_dir.mkdir()
    (watched_dir / "a.txt").write_text("a")

    watcher = DiskWatcher(str(watched_dir), conn=temp_db)
    watcher.archive_existing_files()
    first_stats = watcher.scan_stats.copy()

    (watched_dir / "b.txt").write_text("b")
    watcher.archive_existing_files()
    second_stats = watcher.scan_stats

    assert second_stats["status"] == "complete"
    assert second_stats["files_scanned"] >= first_stats["files_scanned"]
    assert second_stats["completed_at"] != first_stats.get("completed_at")


def test_manager_status_reflects_scan_stats(tmp_path, monkeypatch):
    db_root = tmp_path / ".diskwatcher"
    monkeypatch.setattr(db_connection, "DB_DIR", db_root, raising=False)
    monkeypatch.setattr(db_connection, "DB_PATH", db_root / "diskwatcher.db", raising=False)
    monkeypatch.setenv("DISKWATCHER_CONFIG_DIR", str(db_root))
    monkeypatch.setattr(
        "diskwatcher.core.manager.get_mount_info",
        lambda path: {
            "uuid": "test-vol",
            "label": "",
            "device": str(path),
            "volume_id": "test-vol",
        },
        raising=False,
    )

    manager = DiskWatcherManager()
    watched_dir = tmp_path / "watched"
    watched_dir.mkdir()
    (watched_dir / "existing.txt").write_text("content")

    manager.add_directory(watched_dir, uuid="test-vol")
    initial_status = manager.status()
    assert initial_status[0]["scan"]["status"] == "pending"

    manager.run_initial_scans(parallel=False)

    updated_status = manager.status()
    scan_info = updated_status[0]["scan"]
    assert scan_info["status"] == "complete"
    assert scan_info["files_scanned"] >= 1

    with manager.conn:
        rows = manager.conn.execute("SELECT status FROM jobs WHERE job_type = 'initial_scan'").fetchall()
    assert rows
    assert rows[0][0] == "complete"


def test_manager_run_initial_scans_parallel(tmp_path, monkeypatch):
    db_root = tmp_path / ".diskwatcher"
    monkeypatch.setattr(db_connection, "DB_DIR", db_root, raising=False)
    monkeypatch.setattr(db_connection, "DB_PATH", db_root / "diskwatcher.db", raising=False)
    monkeypatch.setenv("DISKWATCHER_CONFIG_DIR", str(db_root))

    def fake_mount_info(path: str):
        resolved = Path(path).resolve()
        return {
            "uuid": f"uuid-{resolved.name}",
            "label": resolved.name,
            "device": str(resolved),
            "volume_id": f"uuid-{resolved.name}",
            "lsblk": {"SERIAL": f"SER-{resolved.name}"},
        }

    monkeypatch.setattr(
        "diskwatcher.utils.devices.get_mount_info", fake_mount_info, raising=False
    )
    monkeypatch.setattr(
        "diskwatcher.core.manager.get_mount_info", fake_mount_info, raising=False
    )
    monkeypatch.setattr(
        "diskwatcher.core.watcher.get_mount_info", fake_mount_info, raising=False
    )

    created_executors = []

    class InlineExecutor:
        def __init__(self, max_workers=None):
            self.max_workers = max_workers
            self._futures: list[concurrent.futures.Future] = []
            created_executors.append(self)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):
            future: concurrent.futures.Future = concurrent.futures.Future()
            try:
                result = fn(*args, **kwargs)
            except Exception as exc:  # pragma: no cover - defensive guard
                future.set_exception(exc)
            else:
                future.set_result(result)
            self._futures.append(future)
            return future

    monkeypatch.setattr(
        "diskwatcher.core.manager.ProcessPoolExecutor", InlineExecutor, raising=False
    )

    manager = DiskWatcherManager()

    first = tmp_path / "disk_a"
    second = tmp_path / "disk_b"
    first.mkdir()
    second.mkdir()
    (first / "file1.txt").write_text("a")
    (second / "file2.txt").write_text("b")

    manager.add_directory(first)
    manager.add_directory(second)

    results = manager.run_initial_scans(parallel=True, max_workers=1)

    assert len(results) == 2
    statuses = {stat["uuid"]: stat["status"] for stat in results}
    assert statuses["uuid-disk_a"] == "complete"
    assert statuses["uuid-disk_b"] == "complete"

    for thread in manager.threads:
        assert thread.watcher.scan_stats["status"] == "complete"
        assert thread.watcher.scan_stats["files_scanned"] >= 1

    events = query_events(manager.conn, limit=10)
    assert {event["volume_id"] for event in events} == {
        "uuid-disk_a",
        "uuid-disk_b",
    }
    assert created_executors and created_executors[0].max_workers == 1

    with manager.conn:
        job_rows = manager.conn.execute(
            "SELECT status FROM jobs WHERE job_type = 'initial_scan' ORDER BY job_id"
        ).fetchall()
    assert len(job_rows) == 2
    assert all(row[0] == "complete" for row in job_rows)


def test_auto_discovery_scans_and_removes(tmp_path, monkeypatch):
    db_root = tmp_path / ".diskwatcher"
    monkeypatch.setattr(db_connection, "DB_DIR", db_root, raising=False)
    monkeypatch.setattr(db_connection, "DB_PATH", db_root / "diskwatcher.db", raising=False)
    monkeypatch.setenv("DISKWATCHER_CONFIG_DIR", str(db_root))

    active_mounts: set[Path] = set()

    def fake_ismount(candidate: Path) -> bool:
        return Path(candidate) in active_mounts

    monkeypatch.setattr(os.path, "ismount", fake_ismount)

    manager = DiskWatcherManager()

    auto_root = tmp_path / "media"
    auto_root.mkdir()

    discovery = _AutoDiscoveryThread(
        manager=manager,
        roots=[auto_root],
        scan_new=True,
        max_workers=1,
        interval=1.0,
    )

    # Initial pass with no disks should leave manager empty.
    discovery.scan_once()
    assert manager.current_paths() == []

    first_disk = auto_root / "disk_one"
    first_disk.mkdir()
    (first_disk / "file.txt").write_text("content")
    resolved_disk = first_disk.resolve()
    active_mounts.add(resolved_disk)

    discovery.scan_once()

    current_paths = manager.current_paths()
    assert resolved_disk in current_paths

    # Removing the directory should stop and deregister the watcher.
    active_mounts.clear()
    shutil.rmtree(first_disk)
    discovery.scan_once()
    assert resolved_disk not in manager.current_paths()
    discovery.stop()


def test_watcher_jobs_recorded(tmp_path, monkeypatch):
    db_root = tmp_path / ".diskwatcher"
    monkeypatch.setattr(db_connection, "DB_DIR", db_root, raising=False)
    monkeypatch.setattr(db_connection, "DB_PATH", db_root / "diskwatcher.db", raising=False)

    started_watchers = []

    def fake_start(self, recursive=True, run_once=False, stop_event=None, job_tracker=None):
        started_watchers.append(self.path)
        if job_tracker:
            job_tracker.update(status="running")
        if stop_event:
            stop_event.set()

    monkeypatch.setattr(DiskWatcher, "start", fake_start)

    manager = DiskWatcherManager()
    watched_dir = tmp_path / "watched"
    watched_dir.mkdir()
    manager.add_directory(watched_dir, uuid="vol-watcher")

    manager.start_all()

    with manager.conn:
        rows = manager.conn.execute(
            "SELECT status FROM jobs WHERE job_type = 'watcher'"
        ).fetchall()
    assert rows

    manager.stop_all()

    with init_db(path=db_root / "diskwatcher.db") as conn:
        final_rows = conn.execute(
            "SELECT status FROM jobs WHERE job_type = 'watcher'"
        ).fetchall()
    assert final_rows
    assert final_rows[0][0] == "stopped"


def test_auto_discovery_parallelizes_new_scans(tmp_path, monkeypatch):
    db_root = tmp_path / ".diskwatcher"
    monkeypatch.setattr(db_connection, "DB_DIR", db_root, raising=False)
    monkeypatch.setattr(db_connection, "DB_PATH", db_root / "diskwatcher.db", raising=False)

    active_mounts: set[Path] = set()

    def fake_ismount(candidate: Path) -> bool:
        return Path(candidate) in active_mounts

    monkeypatch.setattr(os.path, "ismount", fake_ismount)

    manager = DiskWatcherManager()
    # Pretend watchers are already running so discovery triggers asynchronous scans.
    manager._running = True  # type: ignore[attr-defined]

    monkeypatch.setattr(
        DiskWatcherThread,
        "start",
        lambda self: None,
        raising=False,
    )

    auto_root = tmp_path / "media"
    auto_root.mkdir()

    first_disk = auto_root / "disk_a"
    second_disk = auto_root / "disk_b"
    first_disk.mkdir()
    second_disk.mkdir()
    active_mounts.update({first_disk.resolve(), second_disk.resolve()})

    captured_calls = []

    original_run = manager.run_initial_scans

    def fake_run_initial_scans(**kwargs):
        captured_calls.append(kwargs)
        return []

    monkeypatch.setattr(manager, "run_initial_scans", fake_run_initial_scans)

    discovery = _AutoDiscoveryThread(
        manager=manager,
        roots=[auto_root],
        scan_new=True,
        max_workers=4,
        interval=1.0,
    )

    discovery.scan_once()
    discovery.stop()

    assert captured_calls, "Expected auto-discovery to trigger a scan"
    call = captured_calls[0]
    assert call["parallel"] is True
    assert len(call["threads"]) == 2

    monkeypatch.setattr(manager, "run_initial_scans", original_run)


def test_manager_reuses_single_connection(tmp_path, monkeypatch):
    db_root = tmp_path / ".diskwatcher"
    monkeypatch.setattr(db_connection, "DB_DIR", db_root, raising=False)
    monkeypatch.setattr(db_connection, "DB_PATH", db_root / "diskwatcher.db", raising=False)

    monkeypatch.setattr(
        "diskwatcher.core.manager.get_mount_info",
        lambda path: {
            "uuid": "test-vol",
            "label": "",
            "device": str(path),
            "volume_id": "test-vol",
        },
        raising=False,
    )

    manager = DiskWatcherManager()
    try:
        watched_dir = tmp_path / "watched"
        watched_dir.mkdir()

        manager.add_directory(watched_dir, uuid="test-vol")
        assert len(manager.threads) == 1
        thread = manager.threads[0]

        assert thread.conn is manager.conn
        assert thread.watcher.conn is manager.conn
        assert thread.watcher.conn_lock is manager.conn_lock

        test_path = watched_dir / "manual.txt"
        thread.watcher.log_event("created", str(test_path))

        with manager.conn:
            rows = manager.conn.execute(
                "SELECT event_type, path FROM events WHERE path = ?", (str(test_path),)
            ).fetchall()
        assert rows and rows[0][0] == "created"
    finally:
        manager.threads.clear()
        manager.stop_all()


def test_disk_watcher_captures_mount_metadata_once(monkeypatch, temp_db):
    calls = []

    def fake_get_mount_info(path):
        calls.append(path)
        return {
            "directory": path,
            "mount_point": path,
            "device": path,
            "volume_id": "vol-single",
            "uuid": "uuid-single",
            "lsblk": {"SERIAL": "SER1"},
        }

    monkeypatch.setattr("diskwatcher.core.watcher.get_mount_info", fake_get_mount_info)

    watcher = DiskWatcher("/tmp", conn=temp_db)
    assert len(calls) == 1

    watcher.log_event("created", "/tmp/example.txt")
    assert len(calls) == 1


def test_disk_watcher_backoff_retries_until_complete(monkeypatch, temp_db):
    class Monotonic:
        def __init__(self, values):
            self.values = iter(values)
            self.last = 0.0

        def __call__(self):
            try:
                self.last = next(self.values)
            except StopIteration:
                pass
            return self.last

    monotonic = Monotonic([0.0, 10.0, 400.0])
    monkeypatch.setattr("diskwatcher.core.watcher.time.monotonic", monotonic)

    call_count = 0

    def fake_get_mount_info(path):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {
                "directory": path,
                "mount_point": path,
                "device": path,
                "volume_id": "vol-retry",
                "lsblk": None,
            }
        return {
            "directory": path,
            "mount_point": path,
            "device": path,
            "volume_id": "vol-retry",
            "uuid": "uuid-final",
            "lsblk": {"SERIAL": "SER2"},
        }

    monkeypatch.setattr("diskwatcher.core.watcher.get_mount_info", fake_get_mount_info)

    watcher = DiskWatcher("/tmp", conn=temp_db)
    assert call_count == 1

    # Monotonic returns 10.0 -> scheduled refresh not reached
    watcher.log_event("created", "/tmp/example.txt")
    assert call_count == 1

    # Advance time to trigger retry (400.0)
    watcher._refresh_mount_metadata()
    assert call_count == 2
