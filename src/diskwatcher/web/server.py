"""Flask application exposing the DiskWatcher status dashboard."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Tuple, List

from flask import Flask, jsonify, render_template, request

from diskwatcher.db import (
    fetch_jobs,
    fetch_volume_metadata,
    init_db,
    init_db_readonly,
    query_events,
    summarize_by_volume,
)
from diskwatcher.utils.labels import build_label_rows


def _combine_volume_data(
    aggregates: list[dict[str, Any]],
    metadata: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    meta_by_id = {row["volume_id"]: row for row in metadata}
    combined: list[dict[str, Any]] = []
    seen: set[str] = set()

    for agg in aggregates:
        volume_id = agg["volume_id"]
        meta = meta_by_id.get(volume_id, {})
        combined.append({**meta, **agg})
        seen.add(volume_id)

    for meta in metadata:
        volume_id = meta["volume_id"]
        if volume_id in seen:
            continue
        combined.append(meta)

    return combined


def _normalize_jobs(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for job in jobs:
        progress = job.get("progress_json")
        if progress:
            try:
                job["progress"] = json.loads(progress)
            except json.JSONDecodeError:
                job["progress"] = {}
        else:
            job["progress"] = {}
        normalized.append(job)
    return normalized


@contextmanager
def _open_catalog() -> Any:
    """Open the catalog in read-only mode, falling back when absent."""

    try:
        conn = init_db_readonly()
    except sqlite3.OperationalError:
        # First-run catalogs may not exist yet; create/open in writable mode.
        with init_db() as writable_conn:
            yield writable_conn
        return

    try:
        yield conn
    finally:
        conn.close()


def _snapshot(limit: int = 25) -> Tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    try:
        with _open_catalog() as conn:
            events = query_events(conn, limit=limit)
            aggregates = summarize_by_volume(conn)
            volume_meta = fetch_volume_metadata(conn)
            jobs = fetch_jobs(conn)
    except sqlite3.OperationalError:
        return [], [], []

    combined = _combine_volume_data(aggregates, volume_meta)
    normalized_jobs = _normalize_jobs(jobs)
    return events, combined, normalized_jobs


def create_app(*, refresh_seconds: int = 5, event_limit: int = 25) -> Flask:
    """Return a configured Flask application for the dashboard."""

    app = Flask(__name__, template_folder="templates")
    app.config["REFRESH_SECONDS"] = refresh_seconds
    app.config["EVENT_LIMIT"] = event_limit

    @app.route("/")
    def dashboard() -> str:
        events, volumes, jobs = _snapshot(limit=app.config["EVENT_LIMIT"])
        return render_template(
            "status.html",
            updated_at=datetime.now(timezone.utc),
            refresh_seconds=app.config["REFRESH_SECONDS"],
            events=events,
            volumes=volumes,
            jobs=jobs,
        )

    @app.route("/api/status")
    def status_api() -> Any:
        events, volumes, jobs = _snapshot(limit=app.config["EVENT_LIMIT"])
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "events": events,
            "volumes": volumes,
            "jobs": jobs,
        }
        return jsonify(payload)

    @app.route("/api/volumes")
    def volumes_api() -> Any:
        """Return volume metadata rows suitable for remote agents."""
        try:
            with _open_catalog() as conn:
                records = fetch_volume_metadata(conn)
        except sqlite3.OperationalError:
            records = []

        rows = build_label_rows(records)
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "volumes": rows,
        }
        return jsonify(payload)

    @app.route("/api/volumes/by-path")
    def volume_by_path_api() -> Any:
        """Return a single volume row matching the given directory path."""
        path = request.args.get("path")
        if not path:
            return jsonify({"error": "Missing 'path' query parameter"}), 400

        try:
            with _open_catalog() as conn:
                records = fetch_volume_metadata(conn)
        except sqlite3.OperationalError:
            records = []

        matched: List[Dict[str, Any]] = []
        for record in records:
            directory = record.get("directory")
            if directory and str(directory) == path:
                matched.append(record)

        if not matched:
            return jsonify({"error": "No volume found for path", "path": path}), 404

        rows = build_label_rows(matched)
        # We expect a single row; return the first for convenience.
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "volume": rows[0],
        }
        return jsonify(payload)

    return app


__all__ = ["create_app"]
