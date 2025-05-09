import pytest
import sqlite3
from datetime import datetime
from diskwatcher.db import create_schema, log_event, query_events


@pytest.fixture
def db_conn():
    """Create an in-memory SQLite DB with schema loaded."""
    conn = sqlite3.connect(":memory:")
    create_schema(conn)
    yield conn
    conn.close()


def test_log_and_query_event(db_conn):
    log_event(
        db_conn,
        event_type="created",
        path="/tmp/testfile.txt",
        directory="/tmp",
        volume_id="test-volume",
        process_id="12345",
        timestamp="2025-01-01T12:00:00Z",
    )

    events = query_events(db_conn)
    assert len(events) == 1
    event = events[0]
    assert event["event_type"] == "created"
    assert event["path"] == "/tmp/testfile.txt"
    assert event["volume_id"] == "test-volume"
    assert event["process_id"] == "12345"
    assert event["timestamp"] == "2025-01-01T12:00:00Z"


def test_event_auto_timestamp(db_conn):
    log_event(
        db_conn,
        event_type="deleted",
        path="/tmp/file.txt",
        directory="/tmp",
        volume_id="vol-2",
        process_id="999",
    )

    events = query_events(db_conn)
    assert len(events) == 1
    assert events[0]["event_type"] == "deleted"
    assert events[0]["timestamp"]  # should be a real timestamp string
