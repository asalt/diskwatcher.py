import logging
from pathlib import Path

from diskwatcher.utils import logging as diskwatcher_logging


def test_setup_logging_falls_back_to_cwd(monkeypatch, tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    monkeypatch.chdir(run_dir)

    primary_dir = tmp_path / "primary"
    primary_dir.mkdir()
    primary_log = primary_dir / "diskwatcher.log"

    monkeypatch.setattr(diskwatcher_logging, "LOG_DIR", primary_dir)
    monkeypatch.setattr(diskwatcher_logging, "LOG_FILE", primary_log)

    real_file_handler = diskwatcher_logging.logging.FileHandler

    def _file_handler(path, *args, **kwargs):
        if Path(path) == primary_log:
            raise PermissionError("Permission denied")
        return real_file_handler(path, *args, **kwargs)

    monkeypatch.setattr(diskwatcher_logging.logging, "FileHandler", _file_handler)

    diskwatcher_logging.setup_logging(level=logging.INFO)

    expected_dir = run_dir / ".diskwatcher_logs"
    expected_file = expected_dir / "diskwatcher.log"

    assert diskwatcher_logging.active_log_dir() == expected_dir
    assert diskwatcher_logging.active_log_file() == expected_file
    assert expected_file.exists()


def test_setup_logging_disables_file_logging_when_unwritable(monkeypatch, tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    monkeypatch.chdir(run_dir)

    primary_dir = tmp_path / "primary"
    primary_dir.mkdir()
    primary_log = primary_dir / "diskwatcher.log"

    monkeypatch.setattr(diskwatcher_logging, "LOG_DIR", primary_dir)
    monkeypatch.setattr(diskwatcher_logging, "LOG_FILE", primary_log)

    def _file_handler(*args, **kwargs):
        raise PermissionError("Permission denied")

    monkeypatch.setattr(diskwatcher_logging.logging, "FileHandler", _file_handler)

    diskwatcher_logging.setup_logging(level=logging.INFO)

    assert diskwatcher_logging.active_log_dir() is None
    assert diskwatcher_logging.active_log_file() is None
