import sqlite3
from pathlib import Path

import pytest

import diskwatcher.db.connection as db_connection
from diskwatcher.db import init_db, log_event
from diskwatcher.db.jobs import JobHandle

pytest.importorskip("flask")

from diskwatcher.web.server import create_app


@pytest.fixture
def temp_catalog(tmp_path, monkeypatch):
    db_root = tmp_path / ".diskwatcher"
    monkeypatch.setattr(db_connection, "DB_DIR", db_root, raising=False)
    monkeypatch.setattr(db_connection, "DB_PATH", db_root / "diskwatcher.db", raising=False)

    with init_db() as conn:
        log_event(
            conn,
            event_type="created",
            path=str(tmp_path / "file.txt"),
            directory=str(tmp_path),
            volume_id="vol-web",
            process_id="pid",
        )
        JobHandle.start(
            conn,
            job_type="initial_scan",
            path=str(tmp_path),
            volume_id="vol-web",
            status="running",
        )

    yield db_root


def test_dashboard_routes(temp_catalog):
    app = create_app(refresh_seconds=1, event_limit=5)
    client = app.test_client()

    resp = client.get("/api/status")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert "events" in payload
    assert "volumes" in payload
    assert "jobs" in payload

    resp_html = client.get("/")
    assert resp_html.status_code == 200
    body = resp_html.get_data(as_text=True)
    assert "DiskWatcher Dashboard" in body
    assert "Active Jobs" in body
