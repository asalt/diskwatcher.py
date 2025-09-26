"""Database helpers for catalog events and derived metadata."""

from __future__ import annotations

import json
import logging
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


logger = logging.getLogger(__name__)

_VOLUME_USAGE_REFRESH_SECONDS = 300
_VOLUME_USAGE_REFRESH_EVENT_THRESHOLD = 100
_FILE_IGNORE_SUFFIXES = {".lock", ".tmp", ".swp", ".swx", "~"}
_FILE_IGNORE_NAMES = {".DS_Store", "Thumbs.db"}
_DB_MAX_RETRIES = 3
_DB_RETRY_DELAY_BASE = 0.05



def log_event(
    conn: sqlite3.Connection,
    event_type: str,
    path: str,
    directory: str,
    volume_id: str,
    process_id: Optional[str] = None,
    timestamp: Optional[str] = None,
    mount_metadata: Optional[Dict[str, Any]] = None,
) -> None:
    """Persist an event and refresh derived metadata."""

    if timestamp is None:
        timestamp = datetime.now(timezone.utc).isoformat()

    with conn:
        _execute_with_retry(
            conn,
            """
            INSERT INTO events (timestamp, event_type, path, directory, volume_id, process_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (timestamp, event_type, path, directory, volume_id, process_id),
        )
        try:
            _update_volume_metadata(
                conn,
                volume_id,
                directory,
                event_type,
                timestamp,
                mount_metadata=mount_metadata,
            )
        except Exception:  # pragma: no cover - defensive guard
            logger.exception("Failed to update volume metadata", extra={"volume_id": volume_id})
        try:
            _update_file_metadata(conn, event_type, path, directory, volume_id, timestamp)
        except Exception:  # pragma: no cover - defensive guard
            logger.exception("Failed to update file metadata", extra={"path": path})


def query_events(conn: sqlite3.Connection, limit: int = 100) -> List[Dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM events ORDER BY timestamp DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_volume_metadata(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Return raw metadata stored for each tracked volume."""

    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT
            volume_id,
            directory,
            event_count,
            created_count,
            modified_count,
            deleted_count,
            last_event_timestamp,
            usage_total_bytes,
            usage_used_bytes,
            usage_free_bytes,
            usage_refreshed_at,
            mount_device,
            mount_point,
            mount_uuid,
            mount_label,
            mount_volume_id,
            lsblk_name,
            lsblk_path,
            lsblk_model,
            lsblk_serial,
            lsblk_vendor,
            lsblk_size,
            lsblk_fsver,
            lsblk_pttype,
            lsblk_ptuuid,
            lsblk_parttype,
            lsblk_partuuid,
            lsblk_parttypename,
            lsblk_wwn,
            lsblk_maj_min,
            lsblk_json,
            identity_refreshed_at
        FROM volumes
        ORDER BY (last_event_timestamp IS NULL), last_event_timestamp DESC, volume_id
        """
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


def _update_volume_metadata(
    conn: sqlite3.Connection,
    volume_id: str,
    directory: str,
    event_type: str,
    event_timestamp: str,
    mount_metadata: Optional[Dict[str, Any]] = None,
) -> None:
    created_delta = 1 if event_type == "created" else 0
    modified_delta = 1 if event_type == "modified" else 0
    deleted_delta = 1 if event_type == "deleted" else 0

    _execute_with_retry(
        conn,
        """
        INSERT INTO volumes (volume_id, directory)
        VALUES (?, ?)
        ON CONFLICT(volume_id) DO UPDATE SET directory = excluded.directory
        """,
        (volume_id, directory),
    )

    _execute_with_retry(
        conn,
        """
        UPDATE volumes
        SET event_count = event_count + 1,
            created_count = created_count + ?,
            modified_count = modified_count + ?,
            deleted_count = deleted_count + ?,
            last_event_timestamp = ?,
            events_since_refresh = events_since_refresh + 1
        WHERE volume_id = ?
        """,
        (created_delta, modified_delta, deleted_delta, event_timestamp, volume_id),
    )

    _maybe_refresh_volume_usage(conn, volume_id, directory, event_timestamp)
    if mount_metadata:
        _maybe_persist_volume_identity(
            conn,
            volume_id,
            directory,
            event_timestamp,
            mount_metadata,
        )


def _maybe_refresh_volume_usage(
    conn: sqlite3.Connection,
    volume_id: str,
    directory: str,
    event_timestamp: str,
) -> None:
    row = conn.execute(
        """
        SELECT usage_refreshed_at, events_since_refresh
        FROM volumes
        WHERE volume_id = ?
        """,
        (volume_id,),
    ).fetchone()

    if row is None:
        return

    usage_refreshed_at, events_since_refresh = row

    should_refresh = False
    event_dt = _parse_iso(event_timestamp)

    if usage_refreshed_at is None:
        should_refresh = True
    else:
        last_refresh = _parse_iso(usage_refreshed_at)
        if (event_dt - last_refresh).total_seconds() >= _VOLUME_USAGE_REFRESH_SECONDS:
            should_refresh = True

    if events_since_refresh >= _VOLUME_USAGE_REFRESH_EVENT_THRESHOLD:
        should_refresh = True

    if not should_refresh:
        return

    try:
        usage = shutil.disk_usage(directory)
    except Exception:
        logger.debug(
            "Unable to collect disk usage for directory",
            extra={"directory": directory, "volume_id": volume_id},
        )
        return

    _execute_with_retry(
        conn,
        """
        UPDATE volumes
        SET usage_total_bytes = ?,
            usage_used_bytes = ?,
            usage_free_bytes = ?,
            usage_refreshed_at = ?,
            events_since_refresh = 0
        WHERE volume_id = ?
        """,
        (usage.total, usage.used, usage.free, event_timestamp, volume_id),
    )


def _maybe_persist_volume_identity(
    conn: sqlite3.Connection,
    volume_id: str,
    directory: str,
    event_timestamp: str,
    mount_metadata: Dict[str, Any],
) -> None:
    updates: Dict[str, Any] = {}

    device = mount_metadata.get("device")
    mount_point = mount_metadata.get("mount_point") or directory
    uuid = mount_metadata.get("uuid")
    label = mount_metadata.get("label")
    vol_hint = mount_metadata.get("volume_id")
    lsblk_payload = mount_metadata.get("lsblk") or {}

    if device:
        updates["mount_device"] = device
    if mount_point:
        updates["mount_point"] = mount_point
    if uuid:
        updates["mount_uuid"] = uuid
    if label:
        updates["mount_label"] = label
    if vol_hint and vol_hint != volume_id:
        updates["mount_volume_id"] = vol_hint

    if lsblk_payload:
        updates["lsblk_json"] = json.dumps(lsblk_payload, sort_keys=True)
        for key, column in _LSBLK_COLUMN_MAP.items():
            value = lsblk_payload.get(key)
            if value:
                updates[column] = value

    if not updates:
        return

    updates["identity_refreshed_at"] = event_timestamp

    set_clause = ", ".join(f"{col} = ?" for col in updates)
    values = list(updates.values())

    _execute_with_retry(
        conn,
        f"UPDATE volumes SET {set_clause} WHERE volume_id = ?",
        (*values, volume_id),
    )

def _update_file_metadata(
    conn: sqlite3.Connection,
    event_type: str,
    path: str,
    directory: str,
    volume_id: str,
    event_timestamp: str,
) -> None:
    path_obj = Path(path)
    name = path_obj.name
    if name in _FILE_IGNORE_NAMES:
        return
    for suffix in _FILE_IGNORE_SUFFIXES:
        if name.endswith(suffix):
            return

    if event_type == "deleted":
        _execute_with_retry(
            conn,
            """
            UPDATE files
            SET is_deleted = 1,
                size_bytes = NULL,
                modified_time = NULL,
                last_event_timestamp = ?,
                last_event_type = ?
            WHERE volume_id = ? AND path = ?
            """,
            (event_timestamp, event_type, volume_id, str(path_obj)),
        )
        return

    try:
        stat_result = path_obj.stat()
    except FileNotFoundError:
        return
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.debug("stat() failed for path", extra={"path": path, "error": str(exc)})
        return

    if not path_obj.is_file():
        return

    size_bytes = stat_result.st_size
    modified_time = datetime.fromtimestamp(stat_result.st_mtime, tz=timezone.utc).isoformat()
    created_time = datetime.fromtimestamp(stat_result.st_ctime, tz=timezone.utc).isoformat()

    _execute_with_retry(
        conn,
        """
        INSERT INTO files (
            volume_id,
            path,
            directory,
            size_bytes,
            modified_time,
            created_time,
            last_event_timestamp,
            last_event_type,
            is_deleted
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
        ON CONFLICT(volume_id, path) DO UPDATE SET
            directory = excluded.directory,
            size_bytes = excluded.size_bytes,
            modified_time = excluded.modified_time,
            created_time = COALESCE(files.created_time, excluded.created_time),
            last_event_timestamp = excluded.last_event_timestamp,
            last_event_type = excluded.last_event_type,
            is_deleted = 0
        """,
        (
            volume_id,
            str(path_obj),
            directory,
            size_bytes,
            modified_time,
            created_time,
            event_timestamp,
            event_type,
        ),
    )


def _parse_iso(raw: Optional[str]) -> datetime:
    if raw is None:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return datetime.fromtimestamp(0, tz=timezone.utc)


def _execute_with_retry(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> None:
    for attempt in range(_DB_MAX_RETRIES):
        try:
            conn.execute(sql, params)
            return
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "locked" not in message and "busy" not in message:
                raise
            if attempt == _DB_MAX_RETRIES - 1:
                raise
            time.sleep(_DB_RETRY_DELAY_BASE * (2 ** attempt))
_LSBLK_COLUMN_MAP: Dict[str, str] = {
    "NAME": "lsblk_name",
    "PATH": "lsblk_path",
    "MODEL": "lsblk_model",
    "SERIAL": "lsblk_serial",
    "VENDOR": "lsblk_vendor",
    "SIZE": "lsblk_size",
    "FSVER": "lsblk_fsver",
    "PTTYPE": "lsblk_pttype",
    "PTUUID": "lsblk_ptuuid",
    "PARTTYPE": "lsblk_parttype",
    "PARTUUID": "lsblk_partuuid",
    "PARTTYPENAME": "lsblk_parttypename",
    "WWN": "lsblk_wwn",
    "MAJ:MIN": "lsblk_maj_min",
}
