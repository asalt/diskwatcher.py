import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

from typer.testing import CliRunner

import diskwatcher.db.connection as db_connection
from diskwatcher.core.cli import app
from diskwatcher.db import init_db, log_event
from diskwatcher.utils import config as config_utils
import alembic.command
import sqlite3


def _patch_db(monkeypatch, tmp_path):
    db_root = tmp_path / ".diskwatcher"
    monkeypatch.setattr(db_connection, "DB_DIR", db_root, raising=False)
    monkeypatch.setattr(db_connection, "DB_PATH", db_root / "diskwatcher.db", raising=False)
    monkeypatch.setattr("diskwatcher.core.cli.setup_logging", lambda level=None: None)
    monkeypatch.setenv(config_utils.CONFIG_ENV_VAR, str(tmp_path / ".diskwatcher_config"))
    return db_root


def _mock_mount_info(volume_id: str):
    def _info(directory):
        directory_str = str(directory)
        return {
            "directory": directory_str,
            "mount_point": directory_str,
            "device": "/dev/mock",
            "volume_id": volume_id,
            "uuid": f"{volume_id}-uuid",
            "label": f"{volume_id}-label",
            "identity_refreshed_at": "2025-01-01T00:00:00Z",
            "lsblk": {
                "MODEL": "MockDrive",
                "SERIAL": "MOCK-SERIAL",
                "VENDOR": "MockVendor",
                "SIZE": "1T",
                "PTTYPE": "gpt",
                "PTUUID": "mock-ptuuid",
                "PARTTYPE": "mock-parttype",
                "PARTUUID": "mock-partuuid",
                "PARTTYPENAME": "Mock partition",
                "WWN": "mock-wwn",
                "FSVER": "1.0",
                "MAJ:MIN": "8:18",
            },
        }

    return _info


def _run_cli(args, home, *, config_dir: Optional[Path] = None, env_update: Optional[dict] = None):
    env = os.environ.copy()
    home_path = Path(home)
    env["HOME"] = str(home_path)
    existing = env.get("PYTHONPATH")
    src_path = str((Path(__file__).resolve().parents[1] / "src").resolve())
    env["PYTHONPATH"] = f"{src_path}{os.pathsep}{existing}" if existing else src_path
    config_base = config_dir if config_dir is not None else home_path / ".diskwatcher"
    env[config_utils.CONFIG_ENV_VAR] = str(config_base)
    if env_update:
        env.update(env_update)
    return subprocess.run(
        [sys.executable, "-m", "diskwatcher.core.cli", *args],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


def _stdout_json(output: str) -> dict:
    start = output.find("{")
    assert start != -1, f"No JSON payload in output: {output!r}"
    return json.loads(output[start:])


def test_cli_help_smoke(tmp_path):
    result = _run_cli(["--help"], home=tmp_path)
    assert result.returncode == 0
    assert "DiskWatcher CLI" in result.stdout


def test_cli_log_level_validation(tmp_path):
    result = _run_cli(["--log-level", "verbose", "status"], home=tmp_path)
    assert result.returncode != 0
    assert "Unsupported log level" in result.stderr


def test_config_show_defaults(tmp_path):
    result = _run_cli(["config", "show", "--json"], home=tmp_path)
    assert result.returncode == 0
    payload = _stdout_json(result.stdout)
    options = payload["options"]
    paths = payload["paths"]
    assert options["log.level"]["value"] == "info"
    assert options["log.level"]["source"] == "default"
    assert options["run.auto_scan"]["value"] is True
    assert paths["database_dir"].endswith(".diskwatcher")
    assert paths["database_file"].endswith("diskwatcher.db")


def test_config_set_and_show(tmp_path):
    config_dir = tmp_path / "config"
    set_result = _run_cli(
        ["config", "set", "log.level", "debug"],
        home=tmp_path,
        config_dir=config_dir,
    )
    assert set_result.returncode == 0

    show_result = _run_cli(
        ["config", "show", "--json"],
        home=tmp_path,
        config_dir=config_dir,
    )
    data = _stdout_json(show_result.stdout)
    options = data["options"]
    paths = data["paths"]
    assert options["log.level"]["value"] == "debug"
    assert options["log.level"]["source"] == "user"
    assert Path(paths["config_dir"]).resolve() == config_dir.resolve()


def test_config_set_rejects_unknown_keys(tmp_path):
    result = _run_cli(["config", "set", "unknown.key", "value"], home=tmp_path)
    assert result.returncode != 0
    assert "Unknown config key" in result.stderr


def test_config_show_text_includes_paths(tmp_path):
    result = _run_cli(["config", "show"], home=tmp_path)
    assert result.returncode == 0
    assert "Storage paths:" in result.stdout
    assert "database file" in result.stdout


def test_status_shows_recent_events(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)
    mount_factory = _mock_mount_info("vol-1")
    monkeypatch.setattr(
        "diskwatcher.core.cli.get_mount_info",
        mount_factory,
        raising=False,
    )

    with init_db() as conn:
        mount_metadata = mount_factory(str(tmp_path))
        log_event(
            conn,
            event_type="created",
            path=str(tmp_path / "file.txt"),
            directory=str(tmp_path),
            volume_id="vol-1",
            process_id="pid",
            mount_metadata=mount_metadata,
        )

        mount_metadata = mount_factory(str(tmp_path))
        log_event(
            conn,
            event_type="deleted",
            path=str(tmp_path / "file2.txt"),
            directory=str(tmp_path),
            volume_id="vol-1",
            process_id="pid",
            mount_metadata=mount_metadata,
        )

    runner = CliRunner()
    result = runner.invoke(app, ["status", "--limit", "5"])

    assert result.exit_code == 0
    assert "created" in result.output
    assert "vol-1" in result.output
    assert "By volume:" in result.output
    assert "total=2" in result.output
    assert "Volume metadata:" in result.output
    assert "model=MockDrive" in result.output
    assert "serial=MOCK-SERIAL" in result.output
    assert "identity: refreshed=" in result.output
    assert "source=stored" in result.output


def test_status_handles_empty_catalog(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)

    runner = CliRunner()
    result = runner.invoke(app, ["status"])

    assert result.exit_code == 0
    assert "No events recorded yet." in result.output


def test_status_json_output(monkeypatch, tmp_path):
    _patch_db(monkeypatch, tmp_path)
    mount_factory = _mock_mount_info("vol-json")
    monkeypatch.setattr(
        "diskwatcher.core.cli.get_mount_info",
        mount_factory,
        raising=False,
    )

    with init_db() as conn:
        mount_metadata = mount_factory(str(tmp_path))
        log_event(
            conn,
            event_type="modified",
            path=str(tmp_path / "file.txt"),
            directory=str(tmp_path),
            volume_id="vol-json",
            process_id="pid",
            mount_metadata=mount_metadata,
        )

    runner = CliRunner()
    result = runner.invoke(app, ["status", "--json"])

    assert result.exit_code == 0
    payload = _stdout_json(result.output)
    assert "events" in payload
    volumes = payload["volumes"]
    assert isinstance(volumes, list)
    target = next((row for row in volumes if row["volume_id"] == "vol-json"), None)
    assert target is not None
    assert target["usage_total_bytes"] is not None
    assert target["event_count"] >= 1
    assert target["mount_metadata"]["lsblk"]["MODEL"] == "MockDrive"
    assert target["mount_metadata"]["uuid"] == "vol-json-uuid"
    assert target["mount_metadata"]["identity_refreshed_at"]
    assert target["mount_metadata"]["source"] == "stored"


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
