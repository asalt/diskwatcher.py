import pytest
import sqlite3
from datetime import datetime
from diskwatcher.db import create_schema, log_event, query_events
from diskwatcher.db.events import summarize_by_volume


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


def test_summarize_by_volume(db_conn):
    log_event(
        db_conn,
        event_type="created",
        path="/tmp/new",
        directory="/tmp",
        volume_id="vol-1",
        process_id="123",
        timestamp="2025-01-01T00:00:00Z",
    )
    log_event(
        db_conn,
        event_type="deleted",
        path="/tmp/old",
        directory="/tmp",
        volume_id="vol-1",
        process_id="123",
        timestamp="2025-01-02T00:00:00Z",
    )
    summary = summarize_by_volume(db_conn)
    assert len(summary) == 1
    row = summary[0]
    assert row["volume_id"] == "vol-1"
    assert row["total_events"] == 2
    assert row["created"] == 1
    assert row["deleted"] == 1


def test_volume_metadata_updates(db_conn, tmp_path):
    target_dir = tmp_path / "vol"
    target_dir.mkdir()
    file_path = target_dir / "sample.txt"
    file_path.write_text("hello")

    log_event(
        db_conn,
        event_type="created",
        path=str(file_path),
        directory=str(target_dir),
        volume_id="vol-meta",
        timestamp="2025-01-01T00:00:00Z",
    )

    row = db_conn.execute(
        "SELECT event_count, created_count, usage_total_bytes, usage_refreshed_at FROM volumes WHERE volume_id = ?",
        ("vol-meta",),
    ).fetchone()
    assert row is not None
    assert row[0] == 1
    assert row[1] == 1
    assert row[2] is not None
    assert row[3] == "2025-01-01T00:00:00Z"


def test_file_metadata_tracks_latest_state(db_conn, tmp_path):
    target_dir = tmp_path / "files"
    target_dir.mkdir()
    file_path = target_dir / "document.txt"
    file_path.write_text("revision 1")

    timestamp = "2025-01-03T12:00:00Z"
    log_event(
        db_conn,
        event_type="created",
        path=str(file_path),
        directory=str(target_dir),
        volume_id="vol-files",
        timestamp=timestamp,
    )

    meta = db_conn.execute(
        "SELECT size_bytes, is_deleted, last_event_type FROM files WHERE volume_id = ? AND path = ?",
        ("vol-files", str(file_path)),
    ).fetchone()
    assert meta is not None
    assert meta[0] == len("revision 1")
    assert meta[1] == 0
    assert meta[2] == "created"

    file_path.write_text("revision 2")
    log_event(
        db_conn,
        event_type="modified",
        path=str(file_path),
        directory=str(target_dir),
        volume_id="vol-files",
        timestamp="2025-01-03T12:05:00Z",
    )

    meta = db_conn.execute(
        "SELECT size_bytes, last_event_type, is_deleted FROM files WHERE volume_id = ? AND path = ?",
        ("vol-files", str(file_path)),
    ).fetchone()
    assert meta[0] == len("revision 2")
    assert meta[1] == "modified"
    assert meta[2] == 0

    log_event(
        db_conn,
        event_type="deleted",
        path=str(file_path),
        directory=str(target_dir),
        volume_id="vol-files",
        timestamp="2025-01-03T12:10:00Z",
    )

    meta = db_conn.execute(
        "SELECT size_bytes, last_event_type, is_deleted FROM files WHERE volume_id = ? AND path = ?",
        ("vol-files", str(file_path)),
    ).fetchone()
    assert meta[0] is None
    assert meta[1] == "deleted"
    assert meta[2] == 1


def test_create_schema_fallback_sets_baseline_revision():
    conn = sqlite3.connect(":memory:")
    try:
        create_schema(conn)
        version = conn.execute("SELECT version_num FROM alembic_version").fetchone()
        assert version is not None
        assert version[0] == "0002_volume_and_file_metadata"
    finally:
        conn.close()
