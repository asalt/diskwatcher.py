import json
import os
import time

import pytest
from typer.testing import CliRunner

import diskwatcher.db.connection as db_connection
from diskwatcher.core.cli import app
from diskwatcher.core.manager import DiskWatcherManager
from diskwatcher.db import init_db, query_events


pytestmark = pytest.mark.integration


def test_manager_records_events_and_cli_reports(monkeypatch, artifact_dir):
    """Exercise watcher threads through the manager and surface data via CLI."""

    catalog_path = artifact_dir / "integration_catalog.db"
    monkeypatch.setattr(db_connection, "DB_DIR", catalog_path.parent, raising=False)
    monkeypatch.setattr(db_connection, "DB_PATH", catalog_path, raising=False)
    monkeypatch.setattr(
        "diskwatcher.core.cli.setup_logging", lambda level=None: None, raising=False
    )

    conn = init_db(path=catalog_path, check_same_thread=False)

    try:
        manager = DiskWatcherManager(conn=conn)

        watch_dir = artifact_dir / "watched"
        watch_dir.mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr(
            "diskwatcher.core.manager.get_mount_info",
            lambda path: {
                "uuid": "integration-vol",
                "label": "",
                "device": str(path),
                "volume_id": "integration-vol",
                "lsblk": {"SERIAL": "SER-INT"},
            },
            raising=False,
        )
        monkeypatch.setattr(
            "diskwatcher.core.watcher.get_mount_info",
            lambda path: {
                "directory": str(path),
                "mount_point": str(path),
                "device": str(path),
                "volume_id": "integration-vol",
                "uuid": "integration-vol",
                "lsblk": {"SERIAL": "SER-INT"},
            },
            raising=False,
        )

        try:
            manager.add_directory(watch_dir, uuid="integration-vol")
            manager.start_all()

            time.sleep(0.5)

            file_path = watch_dir / "created.txt"
            file_path.write_text("hello integration")
            os.utime(file_path, None)

            time.sleep(1.2)
        finally:
            manager.stop_all()

        events = query_events(conn, limit=10)
        assert events, "Expected watcher threads to log at least one event"
        assert any(
            event["event_type"] in {"created", "modified"} for event in events
        ), "Watcher events missing expected file activity"

        # Persist the captured events so operators can inspect integration runs.
        events_path = artifact_dir / "events.json"
        events_path.write_text(json.dumps(events, indent=2, sort_keys=True))

        runner = CliRunner()
        result = runner.invoke(app, ["status", "--json", "--limit", "5"])

        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["events"], "CLI status returned no events"
        assert any(
            evt["volume_id"] == "integration-vol" for evt in payload["events"]
        ), "CLI status did not surface integration volume events"
        assert catalog_path.exists(), "Catalog file was not created"

        # Store the CLI response alongside the SQLite catalog for debugging.
        status_path = artifact_dir / "status.json"
        status_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    finally:
        conn.close()
