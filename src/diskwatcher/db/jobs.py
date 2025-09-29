"""Helpers for tracking active jobs in the catalog."""

from __future__ import annotations

import json
import os
import socket
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, Iterable, Optional


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dump_progress(progress: Optional[Dict[str, Any]]) -> Optional[str]:
    if progress is None:
        return None
    return json.dumps(progress, sort_keys=True)


def _execute(conn: sqlite3.Connection, statement: str, params: Iterable[Any], lock: Optional[Lock]) -> None:
    if lock:
        with lock:
            with conn:
                conn.execute(statement, tuple(params))
    else:
        with conn:
            conn.execute(statement, tuple(params))


def create_job(
    conn: sqlite3.Connection,
    *,
    job_type: str,
    path: Optional[str] = None,
    volume_id: Optional[str] = None,
    status: str = "queued",
    progress: Optional[Dict[str, Any]] = None,
    owner_pid: Optional[str] = None,
    owner_host: Optional[str] = None,
    job_id: Optional[str] = None,
    lock: Optional[Lock] = None,
) -> str:
    job_id = job_id or os.urandom(16).hex()
    owner_pid = owner_pid or str(os.getpid())
    owner_host = owner_host or socket.gethostname()
    now = _iso_now()
    _execute(
        conn,
        """
        INSERT INTO jobs
            (job_id, job_type, path, volume_id, status, progress_json,
             owner_pid, owner_host, started_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job_id,
            job_type,
            path,
            volume_id,
            status,
            _dump_progress(progress),
            owner_pid,
            owner_host,
            now,
            now,
        ),
        lock,
    )
    return job_id


def update_job(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    status: Optional[str] = None,
    progress: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    completed: bool = False,
    lock: Optional[Lock] = None,
) -> None:
    assignments = ["updated_at = ?"]
    params: list[Any] = [
        _iso_now(),
    ]

    if status is not None:
        assignments.append("status = ?")
        params.append(status)
    if progress is not None:
        assignments.append("progress_json = ?")
        params.append(_dump_progress(progress))
    if error is not None:
        assignments.append("error_message = ?")
        params.append(error)
    if completed:
        assignments.append("completed_at = ?")
        params.append(_iso_now())

    params.append(job_id)
    statement = f"UPDATE jobs SET {', '.join(assignments)} WHERE job_id = ?"
    _execute(conn, statement, params, lock)


def complete_job(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    status: str = "complete",
    progress: Optional[Dict[str, Any]] = None,
    lock: Optional[Lock] = None,
) -> None:
    update_job(conn, job_id, status=status, progress=progress, completed=True, lock=lock)


def fail_job(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    error: str,
    progress: Optional[Dict[str, Any]] = None,
    lock: Optional[Lock] = None,
) -> None:
    update_job(
        conn,
        job_id,
        status="failed",
        progress=progress,
        error=error,
        completed=True,
        lock=lock,
    )


def touch_job(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    progress: Optional[Dict[str, Any]] = None,
    lock: Optional[Lock] = None,
) -> None:
    update_job(conn, job_id, progress=progress, lock=lock)


def fetch_jobs(
    conn: sqlite3.Connection,
    *,
    include_finished: bool = False,
    limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    if include_finished:
        query = "SELECT * FROM jobs ORDER BY updated_at DESC"
        params: tuple[Any, ...] = ()
    else:
        query = (
            "SELECT * FROM jobs WHERE status NOT IN (" "'complete', 'stopped', 'removed', 'cancelled'" ") "
            "ORDER BY updated_at DESC"
        )
        params = ()

    if limit is not None:
        query += " LIMIT ?"
        params = (*params, limit)

    rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


@dataclass
class JobHandle:
    conn: sqlite3.Connection
    job_id: str
    lock: Optional[Lock] = None

    @classmethod
    def start(
        cls,
        conn: sqlite3.Connection,
        *,
        job_type: str,
        path: Optional[str] = None,
        volume_id: Optional[str] = None,
        status: str = "queued",
        progress: Optional[Dict[str, Any]] = None,
        lock: Optional[Lock] = None,
        job_id: Optional[str] = None,
    ) -> "JobHandle":
        created_id = create_job(
            conn,
            job_type=job_type,
            path=path,
            volume_id=volume_id,
            status=status,
            progress=progress,
            job_id=job_id,
            lock=lock,
        )
        return cls(conn=conn, job_id=created_id, lock=lock)

    @classmethod
    def attach(
        cls,
        conn: sqlite3.Connection,
        job_id: str,
        *,
        lock: Optional[Lock] = None,
    ) -> "JobHandle":
        return cls(conn=conn, job_id=job_id, lock=lock)

    def update(
        self,
        *,
        status: Optional[str] = None,
        progress: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        update_job(
            self.conn,
            self.job_id,
            status=status,
            progress=progress,
            error=error,
            lock=self.lock,
        )

    def heartbeat(self, *, progress: Optional[Dict[str, Any]] = None) -> None:
        touch_job(self.conn, self.job_id, progress=progress, lock=self.lock)

    def complete(
        self,
        *,
        status: str = "complete",
        progress: Optional[Dict[str, Any]] = None,
    ) -> None:
        complete_job(self.conn, self.job_id, status=status, progress=progress, lock=self.lock)

    def fail(
        self,
        *,
        error: str,
        progress: Optional[Dict[str, Any]] = None,
    ) -> None:
        fail_job(self.conn, self.job_id, error=error, progress=progress, lock=self.lock)


__all__ = [
    "JobHandle",
    "create_job",
    "update_job",
    "complete_job",
    "fail_job",
    "touch_job",
    "fetch_jobs",
]
