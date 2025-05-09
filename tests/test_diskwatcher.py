# tests/test_diskwatcher.py
import os
import sqlite3
import pytest
import logging
from diskwatcher.core.watcher import DiskWatcher, DiskWatcherThread
from watchdog.events import FileCreatedEvent, FileModifiedEvent, FileDeletedEvent
import time
import threading
from diskwatcher.db import create_schema, query_events


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
