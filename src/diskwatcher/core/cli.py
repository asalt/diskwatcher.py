import json
import logging
import re
import sys
import time
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, unquote

import typer

from diskwatcher.core.manager import DiskWatcherManager
from diskwatcher.core.inspector import suggest_directories
from diskwatcher.utils import config as config_utils
from diskwatcher.utils.logging import setup_logging, get_logger, LOG_DIR, LOG_FILE
from diskwatcher.utils.devices import get_mount_info
from diskwatcher.db import init_db, query_events, fetch_jobs
from diskwatcher.db.connection import DB_PATH, DB_DIR
from diskwatcher.db.events import (
    summarize_by_volume,
    summarize_files,
    query_events_since,
    fetch_volume_metadata,
)
from diskwatcher.db.migration import upgrade as migrate_upgrade, build_alembic_config


_LOG_LEVEL_CHOICES = {
    name: getattr(logging, name.upper()) for name in config_utils.LOG_LEVEL_VALUES
}
_LOG_LEVEL_CHOICES["warn"] = logging.WARNING

_VOLUME_IDENTITY_COLUMNS = (
    "mount_device",
    "mount_point",
    "mount_uuid",
    "mount_label",
    "mount_volume_id",
    "lsblk_name",
    "lsblk_path",
    "lsblk_model",
    "lsblk_serial",
    "lsblk_vendor",
    "lsblk_size",
    "lsblk_fsver",
    "lsblk_pttype",
    "lsblk_ptuuid",
    "lsblk_parttype",
    "lsblk_partuuid",
    "lsblk_parttypename",
    "lsblk_wwn",
    "lsblk_maj_min",
    "lsblk_json",
    "identity_refreshed_at",
)

_LSBLK_COLUMN_MAP = {
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


app = typer.Typer(help="DiskWatcher CLI - Monitor filesystem events.", no_args_is_help=True)
config_app = typer.Typer(help="Inspect and edit DiskWatcher configuration.")
dev_app = typer.Typer(help="Developer tooling for migrations and catalog upkeep.")
app.add_typer(config_app, name="config")
app.add_typer(dev_app, name="dev")


def _emit_config_error(error: config_utils.ConfigError) -> None:
    typer.echo(f"Configuration error: {error}", err=True)
    raise typer.Exit(code=1)


def _render_config_value(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value)


def _get_config_value(key: str) -> Any:
    try:
        return config_utils.get_value(key)
    except config_utils.ConfigError as exc:
        _emit_config_error(exc)
    raise AssertionError("unreachable")


@app.callback()
def configure_logging(
    log_level: Optional[str] = typer.Option(
        None,
        help="Logging level (config key: log.level).",
        metavar="LEVEL",
    ),
) -> None:
    """Configure logging before executing a sub-command."""

    configured_level = _get_config_value("log.level")

    candidate = log_level.lower() if log_level else configured_level
    candidate = "warning" if candidate == "warn" else candidate

    if candidate not in _LOG_LEVEL_CHOICES:
        choices = ", ".join(sorted(v for v in config_utils.LOG_LEVEL_VALUES))
        raise typer.BadParameter(
            f"Unsupported log level '{candidate}'. Choose from {choices}."
        )

    setup_logging(level=_LOG_LEVEL_CHOICES[candidate])


@app.command()
def run(
    directories: Optional[List[Path]] = typer.Argument(
        None,
        exists=True,
        dir_okay=True,
        file_okay=False,
        resolve_path=True,
        help="Directories to monitor. Leave empty to auto-detect."
    ),
    scan: Optional[bool] = typer.Option(
        None,
        "--scan/--no-scan",
        help="Control the initial archival scan (config key: run.auto_scan).",
    ),
    discover_roots: Optional[List[Path]] = typer.Option(
        None,
        "--discover-root",
        help="Automatically monitor new subdirectories created under this root (repeatable).",
        metavar="PATH",
        dir_okay=True,
        file_okay=False,
        resolve_path=True,
    ),
) -> None:
    """Start monitoring a directory (defaults to auto-detected mount points)."""
    logger = get_logger(__name__)

    perform_scan = _get_config_value("run.auto_scan")
    if scan is not None:
        perform_scan = scan

    def _resolve_path(candidate: Path) -> Path:
        try:
            return candidate.resolve()
        except FileNotFoundError:
            return candidate

    config_roots = _get_config_value("run.auto_discover_roots")
    configured_auto_roots = [Path(root).expanduser() for root in config_roots]

    if discover_roots:
        auto_roots = [_resolve_path(Path(root)) for root in discover_roots]
    elif configured_auto_roots:
        auto_roots = [_resolve_path(root) for root in configured_auto_roots]
    elif directories:
        auto_roots = [_resolve_path(Path(directory)) for directory in directories]
    else:
        auto_roots = []

    manager = DiskWatcherManager()

    if directories:
        for directory in directories:
            manager.add_directory(Path(directory).resolve())
    else:
        if auto_roots:
            logger.info(
                "Auto discovery only run",
                extra={"roots": [str(root) for root in auto_roots]},
            )
        else:
            suggestions = suggest_directories()
            if not suggestions:
                logger.error(
                    "No suitable directories found to monitor. Specify one manually."
                )
                return

            for suggestion in suggestions:
                logger.info(
                    "auto_detected_directory",
                    extra={
                        "directory": str(suggestion.path),
                        "volume_id": suggestion.volume_id,
                    },
                )
                manager.add_directory(suggestion.path, uuid=suggestion.volume_id)

    max_scan_workers = _get_config_value("run.max_scan_workers")

    if auto_roots:
        logger.info(
            "Auto-discovery roots configured",
            extra={"roots": [str(root) for root in auto_roots]},
        )

        manager.enable_auto_discovery(
            auto_roots,
            scan_new=perform_scan,
            max_workers=max_scan_workers,
            start_thread=False,
        )

    if perform_scan:
        directory_count = len(manager.current_paths())
        parallel = directory_count > 1
        if parallel:
            logger.info(
                "Performing initial archival scan using multiprocessing.",
                extra={
                    "directories": directory_count,
                    "parallel": True,
                    "max_workers": max_scan_workers,
                },
            )
        else:
            logger.info(
                "Performing initial archival scan of monitored directories.",
                extra={
                    "directories": directory_count,
                    "parallel": False,
                    "max_workers": 1,
                },
            )
        manager.run_initial_scans(parallel=parallel, max_workers=max_scan_workers)
    else:
        logger.info("Skipping initial archival scan (run.auto_scan disabled).")

    manager.start_all()

    if auto_roots:
        manager.start_auto_discovery_thread()

    logger.info("Running... Press Ctrl+C to stop.")
    counter = 0
    try:
        while True:
            time.sleep(1)
            counter += 1
            if counter % 10 == 0:
                status_snapshot = manager.status()
                logger.debug(
                    "watcher_heartbeat",
                    extra={"status": status_snapshot, "uptime_seconds": counter},
                )
    except KeyboardInterrupt:
        manager.stop_all()

    # watches = []
    # for d in directories:
    #     try:
    #         info = get_mount_info(d)
    #         vol_id = info["uuid"] or info["label"] or info["device"]
    #     except Exception as e:
    #         logger.warning(f"Could not resolve ID for {d}: {e}")
    #         vol_id = str(d)
    #     watches.append((d, vol_id))
    # logger.info(f"Watching {len(watches)} directories")

    # threads = []
    # for directory, vol_id in watches:
    #     logger.info(f"Watching {vol_id} : {directory}")
    #     watcher = DiskWatcher(directory, uuid=vol_id)
    #     threads.append(watcher)
    #     watcher.start()
    #     threads.append(watcher)

    # try:
    #     while True:
    #         time.sleep(1)
    # except KeyboardInterrupt:
    #     logger.info("Stopping all watchers...")
    # finally:
    #     for t in threads:
    #         t.stop()
    #     for t in threads:
    #         t.join()

    # uuid = None
    # try:
    #     info = get_mount_info(directory)
    #     uuid = info["uuid"] or info["label"] or info["device"]
    # except Exception as e:
    #     logger.warning(f"Could not resolve volume UUID: {e}")
    #     uuid = directory

    # logger.info(f"Starting DiskWatcher on {directory}")
    # watcher = DiskWatcher(directory, uuid=uuid)
    # watcher.start()


@config_app.command("show")
def config_show(
    as_json: bool = typer.Option(False, "--json", help="Emit configuration as JSON."),
) -> None:
    """Display the effective configuration values and their defaults."""

    try:
        data = config_utils.list_config()
    except config_utils.ConfigError as exc:
        _emit_config_error(exc)

    storage_paths = {
        "config_dir": str(config_utils.config_dir()),
        "config_file": str(config_utils.config_path()),
        "database_dir": str(DB_DIR),
        "database_file": str(DB_PATH),
        "log_dir": str(LOG_DIR),
        "log_file": str(LOG_FILE),
    }

    if as_json:
        payload = {"options": data, "paths": storage_paths}
        typer.echo(json.dumps(payload, indent=2))
        return

    for key in sorted(data):
        info = data[key]
        typer.echo(key)
        typer.echo(f"  value   : {_render_config_value(info['value'])} ({info['source']})")
        typer.echo(f"  default : {_render_config_value(info['default'])}")
        typer.echo(f"  type    : {info['type']}")
        if info["choices"]:
            typer.echo(f"  choices : {', '.join(info['choices'])}")
        typer.echo(f"  desc    : {info['description']}")
        typer.echo("")

    typer.echo("Storage paths:")
    for label, value in storage_paths.items():
        typer.echo(f"  {label.replace('_', ' '):<13} : {value}")


@config_app.command("set")
def config_set(key: str, value: str) -> None:
    """Persist a configuration value."""

    try:
        parsed = config_utils.set_value(key, value)
    except config_utils.ConfigError as exc:
        _emit_config_error(exc)

    typer.echo(f"{key} = {_render_config_value(parsed)}")


@config_app.command("unset")
def config_unset(key: str) -> None:
    """Remove an override and fall back to the default."""

    try:
        config_utils.unset_value(key)
    except config_utils.ConfigError as exc:
        _emit_config_error(exc)

    typer.echo(f"Reset {key} to its default value")


@config_app.command("path")
def config_path_cmd() -> None:
    """Show where the configuration file lives."""

    typer.echo(str(config_utils.config_path()))


@app.command()
def log() -> None:
    """Show recent log entries"""
    if LOG_FILE.exists():
        typer.echo(LOG_FILE.read_text())
    else:
        typer.echo("No logs found.")


@app.command()
def status(
    limit: int = typer.Option(10, help="Number of recent events to display."),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON instead of text."),
) -> None:
    """Show a snapshot of recent catalog activity."""
    try:
        with init_db() as conn:
            events = query_events(conn, limit=limit)
            aggregates = summarize_by_volume(conn)
            volume_meta = fetch_volume_metadata(conn)
            jobs = fetch_jobs(conn)
    except sqlite3.OperationalError:
        typer.echo("Catalog is empty. Run `diskwatcher run` to start logging events.")
        return

    combined_volumes = _combine_volume_data(aggregates, volume_meta)
    combined_volumes = _attach_mount_details(combined_volumes)

    if as_json:
        payload = {"events": events, "volumes": combined_volumes, "jobs": jobs}
        typer.echo(json.dumps(payload, indent=2))
        return

    if not events:
        typer.echo("No events recorded yet.")
    else:
        typer.echo("Recent events:")
        for event in events:
            typer.echo(
                f"{event['timestamp']} | {event['event_type']:>8} | {event['volume_id']} | {event['path']}"
            )

    if jobs:
        typer.echo("\nActive jobs:")
        for job in jobs:
            progress = job.get("progress_json")
            if progress:
                try:
                    progress_data = json.loads(progress)
                except json.JSONDecodeError:
                    progress_data = {}
            else:
                progress_data = {}
            job_line = (
                f"{job['job_id'][:8]} {job['job_type']} {job.get('status','')}"
                f" {job.get('path') or job.get('volume_id','')}"
            )
            typer.echo(job_line)
            if progress_data:
                progress_fragments = ", ".join(
                    f"{key}={value}" for key, value in progress_data.items() if key not in {"uuid", "path"}
                )
                if progress_fragments:
                    typer.echo(f"    progress: {progress_fragments}")

    if combined_volumes:
        typer.echo("\nBy volume:")
        for agg in combined_volumes:
            total = agg.get("total_events", agg.get("event_count", 0))
            typer.echo(
                f"{agg['volume_id']} @ {agg['directory']} => total={total}"
                f" (created={agg['created']}, modified={agg['modified']}, deleted={agg['deleted']})"
            )

        typer.echo("\nVolume metadata:")
        for meta in combined_volumes:
            typer.echo(f"{meta['volume_id']} @ {meta['directory']}")
            typer.echo(
                "  events : "
                f"stored={meta['event_count']} created={meta['created_count']} "
                f"modified={meta['modified_count']} deleted={meta['deleted_count']}"
            )
            typer.echo(f"  usage  : {_format_usage_line(meta)}")
            mount = meta.get("mount_metadata") or {}
            mount_line = _format_details_line(
                "  mount  : ",
                {
                    "device": mount.get("device"),
                    "mount": mount.get("mount_point"),
                },
            )
            ids_line = _format_details_line(
                "  ids    : ",
                {
                    "volume": mount.get("volume_id")
                    if mount.get("volume_id") and mount.get("volume_id") != meta["volume_id"]
                    else None,
                    "uuid": mount.get("uuid"),
                    "label": mount.get("label"),
                },
            )
            lsblk = mount.get("lsblk") or {}
            block_line = _format_details_line(
                "  block  : ",
                {
                    "model": lsblk.get("MODEL"),
                    "serial": lsblk.get("SERIAL"),
                    "vendor": lsblk.get("VENDOR"),
                    "size": lsblk.get("SIZE"),
                    "fsver": lsblk.get("FSVER"),
                },
            )
            layout_line = _format_details_line(
                "  layout : ",
                {
                    "pttype": lsblk.get("PTTYPE"),
                    "ptuuid": lsblk.get("PTUUID"),
                    "parttype": lsblk.get("PARTTYPE"),
                    "partuuid": lsblk.get("PARTUUID"),
                    "wwn": lsblk.get("WWN"),
                },
            )
            partname_line = _format_details_line(
                "  part   : ",
                {"name": lsblk.get("PARTTYPENAME")},
            )
            identity_line = _format_details_line(
                "  identity: ",
                {
                    "refreshed": mount.get("identity_refreshed_at"),
                    "source": mount.get("source"),
                },
            )
            for detail in (
                mount_line,
                ids_line,
                block_line,
                layout_line,
                partname_line,
                identity_line,
            ):
                if detail:
                    typer.echo(detail)


def _combine_volume_data(
    aggregates: List[Dict[str, Any]],
    metadata: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    meta_by_id = {row["volume_id"]: row for row in metadata}
    combined: List[Dict[str, Any]] = []
    seen: set[str] = set()

    for agg in aggregates:
        meta = meta_by_id.get(agg["volume_id"])
        combined.append(_merge_volume_row(agg, meta))
        seen.add(agg["volume_id"])

    for meta in metadata:
        if meta["volume_id"] not in seen:
            combined.append(
                _merge_volume_row(
                    {
                        "volume_id": meta["volume_id"],
                        "directory": meta["directory"],
                        "total_events": meta["event_count"],
                        "created": meta["created_count"],
                        "modified": meta["modified_count"],
                        "deleted": meta["deleted_count"],
                    },
                    meta,
                )
            )

    return combined


def _extract_mount_metadata(source: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    mount_device = source.get("mount_device")
    mount_point = source.get("mount_point")
    mount_uuid = source.get("mount_uuid")
    mount_label = source.get("mount_label")
    volume_id_hint = source.get("mount_volume_id")
    identity_refreshed = source.get("identity_refreshed_at")

    lsblk_payload: Optional[Dict[str, Any]] = None
    lsblk_json_raw = source.get("lsblk_json")
    if isinstance(lsblk_json_raw, str) and lsblk_json_raw:
        try:
            lsblk_payload = json.loads(lsblk_json_raw)
        except json.JSONDecodeError:
            lsblk_payload = None

    if lsblk_payload is None:
        lsblk_payload = {}
        for key, column in _LSBLK_COLUMN_MAP.items():
            value = source.get(column)
            if value is not None:
                lsblk_payload[key] = value
        if not lsblk_payload:
            lsblk_payload = None

    if not any([mount_device, mount_point, mount_uuid, mount_label, volume_id_hint, lsblk_payload]):
        return None

    return {
        "device": mount_device,
        "mount_point": mount_point,
        "uuid": mount_uuid,
        "label": mount_label,
        "volume_id": volume_id_hint,
        "lsblk": lsblk_payload,
        "identity_refreshed_at": identity_refreshed,
        "source": source.get("mount_metadata_source", "stored"),
    }


def _attach_mount_details(volumes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not volumes:
        return volumes

    logger = get_logger(__name__)
    cache: Dict[str, Optional[dict]] = {}
    enriched: List[Dict[str, Any]] = []

    for row in volumes:
        directory = row.get("directory")
        mount_info: Optional[dict] = None
        if directory:
            directory_str = str(directory)
            if directory_str in cache:
                mount_info = cache[directory_str]
            else:
                try:
                    mount_info = get_mount_info(directory_str)
                except Exception as exc:  # pragma: no cover - defensive logging
                    logger.debug(
                        "mount_info_lookup_failed",
                        extra={"directory": directory_str, "error": str(exc)},
                    )
                    mount_info = None
                cache[directory_str] = mount_info

        updated = dict(row)
        mount_info = updated.get("mount_metadata")
        if not mount_info:
            if directory:
                directory_str = str(directory)
                if directory_str in cache:
                    mount_info = cache[directory_str]
                else:
                    try:
                        mount_info = get_mount_info(directory_str)
                        if mount_info:
                            mount_info["source"] = "live"
                            mount_info.setdefault(
                                "identity_refreshed_at",
                                datetime.now(timezone.utc).isoformat(),
                            )
                    except Exception as exc:  # pragma: no cover - defensive logging
                        logger.debug(
                            "mount_info_lookup_failed",
                            extra={"directory": directory_str, "error": str(exc)},
                        )
                        mount_info = None
                    cache[directory_str] = mount_info
        elif directory:
            cache[str(directory)] = mount_info

        if mount_info:
            updated["mount_metadata"] = mount_info
        enriched.append(updated)

    return enriched


def _search_files(
    conn: sqlite3.Connection,
    pattern: str,
    *,
    regex: bool,
    case_sensitive: bool,
    include_deleted: bool,
    limit: int,
) -> List[Dict[str, Any]]:
    params: List[Any] = []
    clause = _build_search_clause("path", pattern, regex=regex, case_sensitive=case_sensitive, params=params)

    where_parts = [clause]
    if not include_deleted:
        where_parts.append("is_deleted = 0")

    sql = (
        "SELECT path, volume_id, directory, last_event_timestamp, last_event_type, size_bytes, is_deleted "
        "FROM files "
        f"WHERE {' AND '.join(where_parts)} "
        "ORDER BY (last_event_timestamp IS NULL), last_event_timestamp DESC, path "
        "LIMIT ?"
    )

    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def _search_directories(
    conn: sqlite3.Connection,
    pattern: str,
    *,
    regex: bool,
    case_sensitive: bool,
    include_deleted: bool,
    limit: int,
) -> List[Dict[str, Any]]:
    params: List[Any] = []
    clause = _build_search_clause("directory", pattern, regex=regex, case_sensitive=case_sensitive, params=params)

    where_parts = [clause]
    if not include_deleted:
        where_parts.append("is_deleted = 0")

    sql = (
        "SELECT "
        "directory, "
        "volume_id, "
        "COUNT(*) AS total_files, "
        "SUM(CASE WHEN is_deleted = 0 THEN 1 ELSE 0 END) AS active_files, "
        "MAX(last_event_timestamp) AS last_seen "
        "FROM files "
        f"WHERE {' AND '.join(where_parts)} "
        "GROUP BY directory, volume_id "
        "ORDER BY (last_seen IS NULL), last_seen DESC, directory "
        "LIMIT ?"
    )

    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def _build_search_clause(
    column: str,
    pattern: str,
    *,
    regex: bool,
    case_sensitive: bool,
    params: List[Any],
) -> str:
    if regex:
        return f"dw_match_pattern({column})"

    like_pattern = _build_like_pattern(pattern)
    params.append(like_pattern)
    if case_sensitive:
        return f"{column} LIKE ? ESCAPE '\\'"
    return f"LOWER({column}) LIKE LOWER(?) ESCAPE '\\'"


def _build_like_pattern(pattern: str) -> str:
    escaped = pattern.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def _merge_volume_row(agg: Dict[str, Any], meta: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    row = dict(agg)
    if meta:
        row.setdefault("directory", meta["directory"])
        row["event_count"] = meta.get("event_count", row.get("total_events", 0))
        row["created_count"] = meta.get("created_count", row.get("created", 0))
        row["modified_count"] = meta.get("modified_count", row.get("modified", 0))
        row["deleted_count"] = meta.get("deleted_count", row.get("deleted", 0))
        row["usage_total_bytes"] = meta.get("usage_total_bytes")
        row["usage_used_bytes"] = meta.get("usage_used_bytes")
        row["usage_free_bytes"] = meta.get("usage_free_bytes")
        row["usage_refreshed_at"] = meta.get("usage_refreshed_at")
        row.setdefault("last_event_timestamp", meta.get("last_event_timestamp"))
        for column in _VOLUME_IDENTITY_COLUMNS:
            if column in meta and meta[column] is not None:
                row[column] = meta[column]
    else:
        row.setdefault("event_count", row.get("total_events", 0))
        row.setdefault("created_count", row.get("created", 0))
        row.setdefault("modified_count", row.get("modified", 0))
        row.setdefault("deleted_count", row.get("deleted", 0))
        row.setdefault("usage_total_bytes", None)
        row.setdefault("usage_used_bytes", None)
        row.setdefault("usage_free_bytes", None)
        row.setdefault("usage_refreshed_at", None)

    for column in _VOLUME_IDENTITY_COLUMNS:
        row.setdefault(column, None)

    identity_source = meta if meta else row
    row["mount_metadata"] = _extract_mount_metadata(identity_source)

    row.setdefault("total_events", row.get("event_count", 0))
    row.setdefault("created", row.get("created_count", 0))
    row.setdefault("modified", row.get("modified_count", 0))
    row.setdefault("deleted", row.get("deleted_count", 0))
    return row


def _format_usage_line(meta: Dict[str, Any]) -> str:
    total = meta.get("usage_total_bytes")
    used = meta.get("usage_used_bytes")
    free = meta.get("usage_free_bytes")
    refreshed = meta.get("usage_refreshed_at") or "-"

    if total in (None, 0) or used is None or free is None:
        return f"unavailable (refreshed {refreshed})"

    percent = (used / total) * 100 if total else 0
    return (
        f"{_format_bytes(used)} / {_format_bytes(total)} ({percent:.1f}% used, free {_format_bytes(free)})"
        f" refreshed {refreshed}"
    )


def _format_details_line(prefix: str, pairs: Dict[str, Optional[str]]) -> Optional[str]:
    values = [f"{key}={value}" for key, value in pairs.items() if value]
    if not values:
        return None
    return f"{prefix}{' '.join(values)}"


def _format_bytes(value: Optional[int]) -> str:
    if value is None:
        return "-"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    amount = float(value)
    idx = 0
    while amount >= 1024 and idx < len(units) - 1:
        amount /= 1024
        idx += 1
    if idx == 0:
        return f"{int(amount)} {units[idx]}"
    return f"{amount:.1f} {units[idx]}"


@app.command()
def dashboard(
    limit: int = typer.Option(20, help="Number of files to display ordered by recent activity."),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON payload instead of text."),
) -> None:
    """Show a compact summary of cataloged files and volumes."""

    try:
        with init_db() as conn:
            files = summarize_files(conn, limit=limit)
            volumes = summarize_by_volume(conn)
    except sqlite3.OperationalError:
        typer.echo("Catalog is empty. Run `diskwatcher run` to start logging events.")
        return

    if as_json:
        payload = {"files": files, "volumes": volumes}
        typer.echo(json.dumps(payload, indent=2))
        return

    if not files:
        typer.echo("No file activity recorded yet.")
        return

    typer.echo("Recent files:")
    for row in files:
        volume = row.get("volume_id") or "-"
        last_event = row.get("last_event_type") or "unknown"
        last_seen = row.get("last_seen") or ""
        total = row.get("total_events") or 0
        path = row.get("path") or ""
        directory = row.get("directory") or ""

        typer.echo(
            f"- {path}\n"
            f"  volume={volume} directory={directory}\n"
            f"  last_event={last_event} at {last_seen} (events={total})"
        )

    if volumes:
        typer.echo("\nBy volume:")
        for agg in volumes:
            typer.echo(
                f"{agg['volume_id']} @ {agg['directory']} => total={agg['total_events']}"
                f" (created={agg['created']}, modified={agg['modified']}, deleted={agg['deleted']})"
            )


@app.command()
def volumes(
    as_json: bool = typer.Option(False, "--json", help="Emit JSON payload instead of text."),
    raw: bool = typer.Option(
        False,
        "--raw",
        help="Include the stored lsblk_json payload in output.",
    ),
) -> None:
    """Show stored volume snapshots and identity metadata."""

    try:
        with init_db() as conn:
            records = fetch_volume_metadata(conn)
    except sqlite3.OperationalError:
        typer.echo("Catalog is empty. Run `diskwatcher run` to start logging events.")
        return

    if not records:
        typer.echo("No volumes recorded yet.")
        return

    if as_json:
        payload = []
        for row in records:
            record = dict(row)
            record["mount_metadata"] = _extract_mount_metadata(record)
            if not raw:
                record.pop("lsblk_json", None)
            payload.append(record)
        typer.echo(json.dumps(payload, indent=2))
        return

    for row in records:
        typer.echo(f"{row['volume_id']} @ {row['directory']}")
        typer.echo(
            "  events : "
            f"total={row['event_count']} created={row['created_count']} "
            f"modified={row['modified_count']} deleted={row['deleted_count']}"
        )
        typer.echo(f"  usage  : {_format_usage_line(row)}")

        mount = _extract_mount_metadata(row) or {}
        mount_line = _format_details_line(
            "  mount  : ",
            {
                "device": mount.get("device"),
                "mount": mount.get("mount_point"),
            },
        )
        ids_line = _format_details_line(
            "  ids    : ",
            {
                "volume": mount.get("volume_id"),
                "uuid": mount.get("uuid"),
                "label": mount.get("label"),
            },
        )
        lsblk = mount.get("lsblk") or {}
        block_line = _format_details_line(
            "  block  : ",
            {
                "model": lsblk.get("MODEL"),
                "serial": lsblk.get("SERIAL"),
                "vendor": lsblk.get("VENDOR"),
                "size": lsblk.get("SIZE"),
                "fsver": lsblk.get("FSVER"),
            },
        )
        layout_line = _format_details_line(
            "  layout : ",
            {
                "pttype": lsblk.get("PTTYPE"),
                "ptuuid": lsblk.get("PTUUID"),
                "parttype": lsblk.get("PARTTYPE"),
                "partuuid": lsblk.get("PARTUUID"),
                "wwn": lsblk.get("WWN"),
            },
        )
        partname_line = _format_details_line(
            "  part   : ",
            {"name": lsblk.get("PARTTYPENAME")},
        )
        identity_line = _format_details_line(
            "  identity: ",
            {
                "refreshed": mount.get("identity_refreshed_at"),
                "source": mount.get("source"),
            },
        )

        for detail in (
            mount_line,
            ids_line,
            block_line,
            layout_line,
            partname_line,
            identity_line,
        ):
            if detail:
                typer.echo(detail)

        if raw and row.get("lsblk_json"):
            typer.echo(f"  lsblk_json: {row['lsblk_json']}")

        typer.echo("")


@app.command()
def search(
    pattern: str = typer.Argument(..., help="Substring or regular expression to match."),
    files: bool = typer.Option(True, "--files/--no-files", help="Include files in the results."),
    directories: bool = typer.Option(False, "--dirs/--no-dirs", help="Include directories in the results."),
    regex: bool = typer.Option(False, "--regex", help="Treat pattern as a regular expression."),
    case_sensitive: bool = typer.Option(False, "--case-sensitive", help="Match with case sensitivity."),
    include_deleted: bool = typer.Option(False, "--include-deleted", help="Include deleted files in results."),
    limit: int = typer.Option(50, help="Maximum rows per section."),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON payload instead of text."),
) -> None:
    """Search the catalog for files and/or directories."""

    if not files and not directories:
        raise typer.BadParameter("Enable files and/or directories to search.")

    try:
        with init_db() as conn:
            conn.row_factory = sqlite3.Row

            if regex:
                flags = 0 if case_sensitive else re.IGNORECASE
                try:
                    compiled = re.compile(pattern, flags)
                except re.error as exc:
                    raise typer.BadParameter(f"Invalid regular expression: {exc}") from exc

                def _match(value: Optional[str]) -> int:
                    return int(bool(value and compiled.search(value)))

                conn.create_function("dw_match_pattern", 1, _match)
            else:
                compiled = None

            file_results = []
            dir_results = []

            if files:
                file_results = _search_files(
                    conn,
                    pattern,
                    regex=bool(compiled),
                    case_sensitive=case_sensitive,
                    include_deleted=include_deleted,
                    limit=limit,
                )

            if directories:
                dir_results = _search_directories(
                    conn,
                    pattern,
                    regex=bool(compiled),
                    case_sensitive=case_sensitive,
                    include_deleted=include_deleted,
                    limit=limit,
                )
    except sqlite3.OperationalError:
        typer.echo("Catalog is empty. Run `diskwatcher run` to start logging events.")
        return

    if as_json:
        payload: Dict[str, Any] = {}
        if files:
            payload["files"] = file_results
        if directories:
            payload["directories"] = dir_results
        typer.echo(json.dumps(payload, indent=2))
        return

    if files:
        if file_results:
            typer.echo("Files:")
            for row in file_results:
                deleted = "yes" if row.get("is_deleted") else "no"
                typer.echo(
                    f"- {row['path']}\n"
                    f"  volume={row['volume_id']} directory={row['directory']}\n"
                    f"  last_event={row['last_event_type'] or '-'} at {row['last_event_timestamp'] or '-'} deleted={deleted}"
                )
        else:
            typer.echo("Files: (no matches)")

    if directories:
        if files:
            typer.echo("")
        if dir_results:
            typer.echo("Directories:")
            for row in dir_results:
                typer.echo(
                    f"- {row['directory']} (volume={row['volume_id']})\n"
                    f"  files={row['total_files']} active={row['active_files']} last_seen={row['last_seen'] or '-'}"
                )
        else:
            typer.echo("Directories: (no matches)")

    if not file_results and not dir_results:
        typer.echo("\nNo matches found.")


@app.command()
def stream(
    limit: int = typer.Option(100, help="Maximum events to read per poll."),
    interval: float = typer.Option(1.0, help="Seconds to wait between polls."),
    max_iterations: int = typer.Option(0, hidden=True, help="Internal: stop after N polls."),
) -> None:
    """Emit new catalog events as NDJSON for piping into tools like VisiData."""

    if limit <= 0:
        raise typer.BadParameter("limit must be greater than zero")
    if interval < 0:
        raise typer.BadParameter("interval cannot be negative")

    try:
        with init_db(check_same_thread=False) as conn:
            last_rowid = 0
            iterations = 0

            while True:
                events = query_events_since(conn, last_rowid=last_rowid, limit=limit)
                if events:
                    for event in events:
                        typer.echo(json.dumps(event))
                    sys.stdout.flush()
                    last_rowid = events[-1]["rowid"]

                iterations += 1
                if max_iterations and iterations >= max_iterations:
                    break

                try:
                    time.sleep(interval)
                except KeyboardInterrupt:
                    break
    except sqlite3.OperationalError:
        typer.echo("Catalog is empty. Run `diskwatcher run` to start logging events.")
    except KeyboardInterrupt:
        pass


@app.command()
def suggest() -> None:
    """Inspect system and suggest directories to monitor."""
    from diskwatcher.core.inspector import suggest_directories

    suggested_dirs = suggest_directories()
    if not suggested_dirs:
        typer.echo("No suitable directories found.")
    else:
        typer.echo("Suggested directories to monitor:")
        for suggestion in suggested_dirs:
            typer.echo(
                f"  - {suggestion.path} (volume_id: {suggestion.volume_id})"
            )


@app.command()
def migrate(
    revision: str = typer.Option("head", help="Revision to upgrade to."),
    url: Optional[str] = typer.Option(None, "--url", help="Override database URL."),
) -> None:
    """Run Alembic migrations against the catalog."""
    migrate_upgrade(revision=revision, database_url=url)
    typer.echo(f"Migrated catalog to {revision}")


@dev_app.command("revision")
def dev_revision(
    message: str = typer.Option(..., "--message", "-m", help="Migration message."),
    autogenerate: bool = typer.Option(False, "--autogenerate", help="Run Alembic autogenerate."),
    url: Optional[str] = typer.Option(None, "--url", help="Database URL for autogenerate."),
    ini: Optional[Path] = typer.Option(None, "--ini", help="Path to Alembic ini file."),
) -> None:
    """Create a new Alembic revision script."""

    config = build_alembic_config(ini_path=ini, database_url=url)
    from alembic import command as alembic_command  # Local import to keep CLI fast when unused.

    alembic_command.revision(config, message=message, autogenerate=autogenerate)
    typer.echo("Created new Alembic revision")


def _sqlite_url_to_path(url: str) -> Path:
    parsed = urlparse(url)
    if parsed.scheme != "sqlite":
        raise typer.BadParameter("Only sqlite URLs are supported for this command.")
    raw_path = parsed.path
    if parsed.netloc and not raw_path:
        raw_path = parsed.netloc
    path = unquote(raw_path)
    if path.startswith("//"):
        path = path[1:]
    return Path(path)


@dev_app.command("vacuum")
def dev_vacuum(
    url: Optional[str] = typer.Option(None, "--url", help="Database URL to vacuum."),
) -> None:
    """Run VACUUM on the catalog to reclaim space."""

    target = url or f"sqlite:///{DB_PATH}"
    db_file = _sqlite_url_to_path(target)
    conn = sqlite3.connect(str(db_file))
    try:
        conn.execute("VACUUM")
    finally:
        conn.close()
    typer.echo(f"Vacuumed catalog at {db_file}")


@dev_app.command("integrity")
def dev_integrity(
    url: Optional[str] = typer.Option(None, "--url", help="Database URL to check."),
) -> None:
    """Run sqlite integrity_check and report the result."""

    target = url or f"sqlite:///{DB_PATH}"
    db_file = _sqlite_url_to_path(target)
    conn = sqlite3.connect(str(db_file))
    try:
        result = conn.execute("PRAGMA integrity_check").fetchone()
    finally:
        conn.close()

    status = result[0] if result else "unknown"
    typer.echo(f"Catalog integrity_check: {status}")


def main() -> None:
    """Console script entrypoint invoked by `diskwatcher` binary."""

    app()


def entrypoint() -> None:
    """Alias for console script entrypoint (packaging compatibility)."""

    main()


if __name__ == "__main__":
    main()
