import json
from typer.testing import CliRunner

import diskwatcher.db.connection as db_connection
from diskwatcher.core.cli import app
from diskwatcher.db import init_db, log_event
import alembic.command
import sqlite3


def _patch_db(monkeypatch, tmp_path):
    db_root = tmp_path / ".diskwatcher"
    monkeypatch.setattr(db_connection, "DB_DIR", db_root, raising=False)
    monkeypatch.setattr(db_connection, "DB_PATH", db_root / "diskwatcher.db", raising=False)
    monkeypatch.setattr("diskwatcher.core.cli.setup_logging", lambda level=None: None)
    return db_root


def test_status_shows_recent_events(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)

    with init_db() as conn:
        log_event(
            conn,
            event_type="created",
            path=str(tmp_path / "file.txt"),
            directory=str(tmp_path),
            volume_id="vol-1",
            process_id="pid",
        )

        log_event(
            conn,
            event_type="deleted",
            path=str(tmp_path / "file2.txt"),
            directory=str(tmp_path),
            volume_id="vol-1",
            process_id="pid",
        )

    runner = CliRunner()
    result = runner.invoke(app, ["status", "--limit", "5"])

    assert result.exit_code == 0
    assert "created" in result.output
    assert "vol-1" in result.output
    assert "By volume:" in result.output
    assert "total=2" in result.output


def test_status_handles_empty_catalog(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)

    runner = CliRunner()
    result = runner.invoke(app, ["status"])

    assert result.exit_code == 0
    assert "No events recorded yet." in result.output


def test_status_json_output(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)

    with init_db() as conn:
        log_event(
            conn,
            event_type="modified",
            path=str(tmp_path / "file.txt"),
            directory=str(tmp_path),
            volume_id="vol-json",
            process_id="pid",
        )

    runner = CliRunner()
    result = runner.invoke(app, ["status", "--json"])

    assert result.exit_code == 0
    assert "\"events\"" in result.output
    assert "vol-json" in result.output


def test_dashboard_lists_recent_files(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)

    file_a = tmp_path / "file_a.txt"
    file_b = tmp_path / "nested" / "file_b.txt"
    file_b.parent.mkdir(parents=True, exist_ok=True)

    with init_db() as conn:
        log_event(
            conn,
            event_type="created",
            path=str(file_a),
            directory=str(tmp_path),
            volume_id="vol-dash",
        )
        log_event(
            conn,
            event_type="modified",
            path=str(file_a),
            directory=str(tmp_path),
            volume_id="vol-dash",
        )
        log_event(
            conn,
            event_type="created",
            path=str(file_b),
            directory=str(file_b.parent),
            volume_id="vol-dash",
        )

    runner = CliRunner()
    result = runner.invoke(app, ["dashboard", "--limit", "5"])

    assert result.exit_code == 0
    assert "Recent files:" in result.output
    assert str(file_a) in result.output
    assert "events=2" in result.output
    assert "volume=vol-dash" in result.output


def test_dashboard_json_output(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)

    with init_db() as conn:
        log_event(
            conn,
            event_type="created",
            path=str(tmp_path / "foo.txt"),
            directory=str(tmp_path),
            volume_id="vol-json",
        )

    runner = CliRunner()
    result = runner.invoke(app, ["dashboard", "--json"])

    assert result.exit_code == 0
    assert "\"files\"" in result.output
    assert "vol-json" in result.output


def test_dashboard_handles_empty_catalog(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)

    runner = CliRunner()
    result = runner.invoke(app, ["dashboard"])

    assert result.exit_code == 0
    assert "No file activity recorded yet." in result.output


def test_stream_outputs_new_events(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)

    with init_db() as conn:
        log_event(
            conn,
            event_type="created",
            path=str(tmp_path / "foo.txt"),
            directory=str(tmp_path),
            volume_id="vol-stream",
        )
        log_event(
            conn,
            event_type="modified",
            path=str(tmp_path / "foo.txt"),
            directory=str(tmp_path),
            volume_id="vol-stream",
        )

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["stream", "--limit", "5", "--interval", "0", "--max-iterations", "1"],
    )

    assert result.exit_code == 0
    lines = [line for line in result.output.splitlines() if line.strip()]
    assert len(lines) == 2
    payload = json.loads(lines[0])
    assert payload["volume_id"] == "vol-stream"
    assert payload["event_type"] == "created"


def test_stream_handles_empty_catalog(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["stream", "--interval", "0", "--max-iterations", "1"],
    )

    assert result.exit_code == 0
    assert result.output.strip() == ""


def test_migrate_invokes_upgrade(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)

    called = {}

    def fake_upgrade(*, revision, database_url):
        called["revision"] = revision
        called["url"] = database_url

    monkeypatch.setattr("diskwatcher.core.cli.migrate_upgrade", fake_upgrade)

    runner = CliRunner()
    result = runner.invoke(app, ["migrate", "--revision", "head", "--url", "sqlite:///custom.db"])

    assert result.exit_code == 0
    assert "Migrated catalog" in result.output
    assert called["revision"] == "head"
    assert called["url"] == "sqlite:///custom.db"


def test_dev_revision_invokes_alembic(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)

    recorded = {}

    def fake_revision(config, message, autogenerate):
        recorded["message"] = message
        recorded["autogenerate"] = autogenerate
        recorded["url"] = config.get_main_option("sqlalchemy.url")

    monkeypatch.setattr(alembic.command, "revision", fake_revision)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "dev",
            "revision",
            "--message",
            "add-things",
            "--autogenerate",
            "--url",
            "sqlite:///tmp/test.db",
        ],
    )

    assert result.exit_code == 0
    assert "Created new Alembic revision" in result.output
    assert recorded["message"] == "add-things"
    assert recorded["autogenerate"] is True
    assert recorded["url"] == "sqlite:///tmp/test.db"


def test_dev_vacuum_and_integrity(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)
    db_path = tmp_path / "cat.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE sample(id INTEGER)")
    conn.commit()
    conn.close()

    runner = CliRunner()
    result_vacuum = runner.invoke(
        app,
        ["dev", "vacuum", "--url", f"sqlite:///{db_path}"],
    )
    assert result_vacuum.exit_code == 0
    assert "Vacuumed catalog" in result_vacuum.output

    result_integrity = runner.invoke(
        app,
        ["dev", "integrity", "--url", f"sqlite:///{db_path}"],
    )
    assert result_integrity.exit_code == 0
    assert "Catalog integrity_check" in result_integrity.output
