import sqlite3
from pathlib import Path

DB_DIR = Path.home() / ".diskwatcher"
DB_PATH = DB_DIR / "diskwatcher.db"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


def init_db(path: Path = DB_PATH) -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    create_schema(conn)
    return conn


def create_schema(conn: sqlite3.Connection, schema_path: Path = SCHEMA_PATH):
    schema = schema_path.read_text()
    with conn:
        conn.executescript(schema)
