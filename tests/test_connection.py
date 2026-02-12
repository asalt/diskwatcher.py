import sqlite3

import pytest

from diskwatcher.db import init_db, init_db_readonly


def test_init_db_readonly_does_not_create_missing_paths(tmp_path):
    missing_dir = tmp_path / "missing"
    missing_db = missing_dir / "diskwatcher.db"

    assert not missing_dir.exists()
    with pytest.raises(sqlite3.OperationalError):
        init_db_readonly(path=missing_db)
    assert not missing_dir.exists()


def test_init_db_readonly_disallows_writes(tmp_path):
    db_path = tmp_path / "diskwatcher.db"
    with init_db(path=db_path) as conn:
        conn.execute("SELECT 1")

    with init_db_readonly(path=db_path) as conn:
        conn.execute("SELECT name FROM sqlite_master").fetchall()
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("CREATE TABLE ro_test (id INTEGER)")
