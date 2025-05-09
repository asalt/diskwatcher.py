# tests/test_diskwatcher.py

import pytest
import logging
from diskwatcher.core.watcher import DiskWatcher
from watchdog.events import FileCreatedEvent, FileModifiedEvent, FileDeletedEvent
import time
import threading

def test_on_created(caplog):
    watcher = DiskWatcher("/tmp")
    event = FileCreatedEvent("/tmp/test.txt")

    with caplog.at_level(logging.INFO):
        watcher.on_created(event)

    assert "File created: /tmp/test.txt" in caplog.text


def test_on_modified(caplog):
    watcher = DiskWatcher("/tmp")
    event = FileModifiedEvent("/tmp/test.txt")

    with caplog.at_level(logging.INFO):
        watcher.on_modified(event)

    assert "File modified: /tmp/test.txt" in caplog.text


def test_on_deleted(caplog):
    watcher = DiskWatcher("/tmp")
    event = FileDeletedEvent("/tmp/test.txt")

    with caplog.at_level(logging.INFO):
        watcher.on_deleted(event)

    assert "File deleted: /tmp/test.txt" in caplog.text



def test_file_create_triggers_event(tmp_path, caplog):
    watcher = DiskWatcher(str(tmp_path))
    stop_event = threading.Event()

    thread = threading.Thread(target=watcher.start, kwargs={'stop_event': stop_event}, daemon=True)
    thread.start()

    time.sleep(1)
    test_file = tmp_path / "testfile.txt"
    test_file.write_text("hi")

    time.sleep(1)
    stop_event.set()
    thread.join()

    assert "File created" in caplog.text
