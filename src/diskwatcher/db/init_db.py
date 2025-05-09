import sqlite3
from datetime import datetime

DB_PATH = Path.home() / ".diskwatcher" / "diskwatcher.db"

def init_db(path=None):
    if path is None:
        path =  DB_PATH
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            event_type TEXT,
            path TEXT,
            directory TEXT,
            volume_id TEXT,
            process_id TEXT
        )
    """)
    conn.commit()
    return conn

