import sqlite3
from pathlib import Path
from typing import Optional

from diskwatcher.utils import config as config_utils

DB_DIR = config_utils.config_dir()
DB_PATH = DB_DIR / "diskwatcher.db"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


def init_db(
    path: Optional[Path] = None,
    *,
    check_same_thread: bool = False,
    isolation_level: Optional[str] = None,
) -> sqlite3.Connection:
    """Initialize the SQLite catalog at the given path, creating schema if needed."""
    target_path = Path(path) if path is not None else DB_PATH
    target_dir = target_path.parent
    target_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(target_path),
        check_same_thread=check_same_thread,
        isolation_level=isolation_level,
        timeout=30.0,
    )
    _configure_connection(conn)
    create_schema(conn)
    return conn


def create_schema(conn: sqlite3.Connection, schema_path: Path = SCHEMA_PATH) -> None:
    """Ensure the catalog schema exists on the provided connection."""

    cursor = conn.execute("PRAGMA database_list")
    db_path = None
    for name, _, path in cursor.fetchall():
        if name == "main" and path not in (None, "", ":memory:"):
            db_path = Path(path)
            break

    if db_path:
        from diskwatcher.db.migration import upgrade  # Local import to avoid cycles

        upgrade(database_url=f"sqlite:///{db_path}")
        return

    # In-memory databases (primarily for tests) fall back to the static schema.
    from diskwatcher.db.migration import BASELINE_REVISION  # Local import avoids cycles

    schema = schema_path.read_text()
    with conn:
        conn.executescript(schema)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS alembic_version (version_num VARCHAR(32) NOT NULL)"
        )
        conn.execute("DELETE FROM alembic_version")
        conn.execute(
            "INSERT INTO alembic_version (version_num) VALUES (?)",
            (BASELINE_REVISION,),
        )


def _configure_connection(conn: sqlite3.Connection) -> None:
    """Apply common pragmas so high-volume writes stay resilient."""

    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 10000")
    try:
        conn.execute("PRAGMA journal_mode = WAL")
    except sqlite3.OperationalError:
        # WAL is not supported for in-memory databases; ignore quietly.
        pass
