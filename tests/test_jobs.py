import sqlite3
from pathlib import Path

from diskwatcher.db import create_schema
from diskwatcher.db.jobs import JobHandle, fetch_jobs


def test_job_lifecycle(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "jobs.db"))
    create_schema(conn)

    handle = JobHandle.start(
        conn,
        job_type="initial_scan",
        path="/media/a",
        volume_id="vol-a",
        status="queued",
    )

    handle.update(status="running", progress={"files_scanned": 10})
    handle.heartbeat(progress={"files_scanned": 20})
    handle.complete(progress={"files_scanned": 40})

    jobs = fetch_jobs(conn, include_finished=True)
    assert len(jobs) == 1
    record = jobs[0]
    assert record["job_type"] == "initial_scan"
    assert record["status"] == "complete"
    assert "files_scanned" in record["progress_json"]
    assert record["completed_at"] is not None

    conn.close()
