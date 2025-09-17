from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import sqlite3


def log_event(
    conn: sqlite3.Connection,
    event_type: str,
    path: str,
    directory: str,
    volume_id: str,
    process_id: Optional[str] = None,
    timestamp: Optional[str] = None,
):
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).isoformat()

    with conn:
        conn.execute(
            """
            INSERT INTO events (timestamp, event_type, path, directory, volume_id, process_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (timestamp, event_type, path, directory, volume_id, process_id),
        )


def query_events(conn: sqlite3.Connection, limit: int = 100) -> List[Dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM events ORDER BY timestamp DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(row) for row in rows]


def summarize_by_volume(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Return aggregate event counts grouped by volume and directory."""

    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT
            volume_id,
            directory,
            COUNT(*) AS total_events,
            SUM(CASE WHEN event_type = 'created' THEN 1 ELSE 0 END) AS created,
            SUM(CASE WHEN event_type = 'modified' THEN 1 ELSE 0 END) AS modified,
            SUM(CASE WHEN event_type = 'deleted' THEN 1 ELSE 0 END) AS deleted,
            MIN(timestamp) AS first_seen,
            MAX(timestamp) AS last_seen
        FROM events
        GROUP BY volume_id, directory
        ORDER BY last_seen DESC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def summarize_files(conn: sqlite3.Connection, limit: int = 20) -> List[Dict[str, Any]]:
    """Return aggregated file activity ordered by most recent change."""

    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT
            e.path,
            e.volume_id,
            e.directory,
            COUNT(*) AS total_events,
            MIN(e.timestamp) AS first_seen,
            MAX(e.timestamp) AS last_seen,
            (
                SELECT latest.event_type
                FROM events AS latest
                WHERE latest.path = e.path
                  AND latest.volume_id = e.volume_id
                ORDER BY latest.timestamp DESC
                LIMIT 1
            ) AS last_event_type
        FROM events AS e
        GROUP BY e.path, e.volume_id, e.directory
        ORDER BY last_seen DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def query_events_since(
    conn: sqlite3.Connection,
    last_rowid: int = 0,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Fetch events with a rowid greater than ``last_rowid``."""

    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT rowid AS rowid, * FROM events WHERE rowid > ? ORDER BY rowid ASC LIMIT ?",
        (last_rowid, limit),
    ).fetchall()
    return [dict(row) for row in rows]
