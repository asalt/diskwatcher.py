import json
import logging
import os
import re
import sys
import time
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, unquote

import typer

from diskwatcher.core.manager import DiskWatcherManager
from diskwatcher.core.inspector import suggest_directories
from diskwatcher.utils import config as config_utils
from diskwatcher.utils.logging import (
    LOG_DIR,
    LOG_FILE,
    active_log_dir,
    active_log_file,
    get_logger,
    setup_logging,
)
from diskwatcher.utils.devices import get_mount_info
from diskwatcher.db import (
    init_db,
    query_events,
    fetch_jobs,
    ensure_volume_label_indices,
)
from diskwatcher.db.connection import DB_PATH, DB_DIR
from diskwatcher.db.events import (
    summarize_by_volume,
    summarize_files,
    query_events_since,
    fetch_volume_metadata,
)
from diskwatcher.db.jobs import cleanup_stale_jobs
from diskwatcher.db.migration import upgrade as migrate_upgrade, build_alembic_config
from diskwatcher.utils.labels import LABEL_EXPORT_COLUMNS, build_label_rows


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

_INITIAL_SCAN_FINAL_STATUSES = {
    "complete",
    "failed",
    "interrupted",
    "cancelled",
    "removed",
    "stopped",
    "stale",
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


def _collect_initial_scan_progress(
    conn: sqlite3.Connection,
    started_at: str,
    *,
    owner_pid: Optional[str] = None,
) -> Dict[str, int]:
    """Aggregate initial scan progress for jobs created at/after started_at."""

    try:
        query = (
            "SELECT status, completed_at, progress_json "
            "FROM jobs "
            "WHERE job_type = 'initial_scan' "
            "AND started_at >= ?"
        )
        params: list[Any] = [started_at]
        if owner_pid is not None:
            query += " AND owner_pid = ?"
            params.append(owner_pid)

        rows = conn.execute(query, tuple(params)).fetchall()
    except sqlite3.Error:
        return {"total": 0, "completed": 0, "running": 0, "failed": 0, "files_scanned": 0}

    total = len(rows)
    completed = 0
    failed = 0
    files_scanned = 0

    for status, completed_at, progress_json in rows:
        if completed_at or status in _INITIAL_SCAN_FINAL_STATUSES:
            completed += 1
        if status == "failed":
            failed += 1

        if not progress_json:
            continue
        try:
            payload = json.loads(progress_json)
        except json.JSONDecodeError:
            continue
        files_value = payload.get("files_scanned")
        if isinstance(files_value, (int, float)):
            files_scanned += max(0, int(files_value))

    running = max(total - completed, 0)
    return {
        "total": total,
        "completed": completed,
        "running": running,
        "failed": failed,
        "files_scanned": files_scanned,
    }


def _render_initial_scan_line(progress: Dict[str, int], tick: int) -> str:
    total = progress["total"]
    completed = progress["completed"]
    running = progress["running"]
    failed = progress["failed"]
    files_scanned = progress["files_scanned"]

    if total <= 0:
        spinner = "|/-\\"[tick % 4]
        return f"initial scan {spinner} preparing jobs..."

    width = 24
    ratio = completed / total if total else 0.0
    filled = min(width, max(0, int(width * ratio)))
    bar = "#" * filled + "-" * (width - filled)
    spinner = "|/-\\"[tick % 4]
    return (
        f"initial scan {spinner} [{bar}] {completed}/{total} drives"
        f" | files={files_scanned:,} | active={running} | failed={failed}"
    )


def _monitor_initial_scan_progress(
    conn: sqlite3.Connection,
    started_at: str,
    stop_event: Event,
    conn_lock: Optional[Lock] = None,
    *,
    interval: float = 0.5,
    owner_pid: Optional[str] = None,
) -> None:
    """Render a lightweight live progress line while initial scans are running."""

    stream = sys.stderr
    interactive = stream.isatty()
    last_progress: Optional[Dict[str, int]] = None
    next_non_tty_emit = time.monotonic()

    last_len = 0
    tick = 0

    tqdm_bar = None
    if interactive:
        try:
            from tqdm import tqdm  # type: ignore

            tqdm_bar = tqdm(
                total=0,
                desc="initial scan",
                unit="drive",
                dynamic_ncols=True,
                leave=True,
            )
        except Exception:
            tqdm_bar = None

    def _snapshot() -> Dict[str, int]:
        if conn_lock:
            with conn_lock:
                return _collect_initial_scan_progress(conn, started_at, owner_pid=owner_pid)
        return _collect_initial_scan_progress(conn, started_at, owner_pid=owner_pid)

    def _emit(progress: Dict[str, int], *, final: bool = False) -> None:
        nonlocal last_len, next_non_tty_emit
        nonlocal tqdm_bar

        if tqdm_bar is not None:
            total = progress["total"]
            completed = progress["completed"]
            if tqdm_bar.total != total:
                tqdm_bar.total = total
            if completed < tqdm_bar.n:
                tqdm_bar.n = completed
            else:
                tqdm_bar.update(completed - tqdm_bar.n)
            tqdm_bar.set_postfix(
                files=f"{progress['files_scanned']:,}",
                active=progress["running"],
                failed=progress["failed"],
                refresh=False,
            )
            tqdm_bar.refresh()
            if final:
                tqdm_bar.close()
            return

        line = _render_initial_scan_line(progress, tick)
        if interactive:
            padded = line + (" " * max(last_len - len(line), 0))
            suffix = "\n" if final else ""
            stream.write(f"\r{padded}{suffix}")
            stream.flush()
            last_len = len(line)
            return

        now = time.monotonic()
        should_emit = final or progress != last_progress or now >= next_non_tty_emit
        if should_emit:
            stream.write(f"{line}\n")
            stream.flush()
            next_non_tty_emit = now + 2.0

    # Render immediately so users see feedback even on short scans.
    initial = _snapshot()
    _emit(initial)
    last_progress = initial

    while not stop_event.wait(interval):
        progress = _snapshot()
        _emit(progress)
        last_progress = progress
        tick += 1

    final_progress = _snapshot()
    _emit(final_progress, final=True)


def _monitor_initial_scan_batches(
    conn: sqlite3.Connection,
    stop_event: Event,
    conn_lock: Optional[Lock] = None,
    *,
    owner_pid: Optional[str] = None,
    interval: float = 0.5,
) -> None:
    """Watch for initial_scan jobs and render progress for each batch.

    Auto-discovery can trigger new initial scans after the first startup scan,
    so this monitor runs for the duration of `diskwatcher run` and only emits
    output when scans are active.
    """

    stream = sys.stderr
    interactive = stream.isatty()

    last_progress: Optional[Dict[str, int]] = None
    next_non_tty_emit = time.monotonic()

    current_batch_started_at: Optional[str] = None
    tick = 0
    last_len = 0

    tqdm_bar = None

    def _create_tqdm_bar() -> None:
        nonlocal tqdm_bar
        if tqdm_bar is not None or not interactive:
            return
        try:
            from tqdm import tqdm  # type: ignore

            tqdm_bar = tqdm(
                total=0,
                desc="initial scan",
                unit="drive",
                dynamic_ncols=True,
                leave=True,
            )
        except Exception:
            tqdm_bar = None

    def _close_tqdm_bar() -> None:
        nonlocal tqdm_bar
        if tqdm_bar is not None:
            tqdm_bar.close()
            tqdm_bar = None

    def _active_batch_start() -> Optional[str]:
        query = (
            "SELECT MIN(started_at) FROM jobs "
            "WHERE job_type = 'initial_scan' "
            "AND completed_at IS NULL"
        )
        params: list[Any] = []
        if owner_pid is not None:
            query += " AND owner_pid = ?"
            params.append(owner_pid)

        if conn_lock:
            with conn_lock:
                row = conn.execute(query, tuple(params)).fetchone()
        else:
            row = conn.execute(query, tuple(params)).fetchone()
        if not row:
            return None
        return row[0]

    def _snapshot(started_at: str) -> Dict[str, int]:
        if conn_lock:
            with conn_lock:
                return _collect_initial_scan_progress(conn, started_at, owner_pid=owner_pid)
        return _collect_initial_scan_progress(conn, started_at, owner_pid=owner_pid)

    def _emit(progress: Dict[str, int], *, final: bool = False) -> None:
        nonlocal last_len, next_non_tty_emit
        nonlocal tqdm_bar
        nonlocal tick

        if tqdm_bar is not None:
            total = progress["total"]
            completed = progress["completed"]
            if tqdm_bar.total != total:
                tqdm_bar.total = total
            if completed < tqdm_bar.n:
                tqdm_bar.n = completed
            else:
                tqdm_bar.update(completed - tqdm_bar.n)
            tqdm_bar.set_postfix(
                files=f"{progress['files_scanned']:,}",
                active=progress["running"],
                failed=progress["failed"],
                refresh=False,
            )
            tqdm_bar.refresh()
            if final:
                _close_tqdm_bar()
            return

        line = _render_initial_scan_line(progress, tick)
        if interactive:
            padded = line + (" " * max(last_len - len(line), 0))
            suffix = "\n" if final else ""
            stream.write(f"\r{padded}{suffix}")
            stream.flush()
            last_len = len(line)
            return

        now = time.monotonic()
        should_emit = final or progress != last_progress or now >= next_non_tty_emit
        if should_emit:
            stream.write(f"{line}\n")
            stream.flush()
            next_non_tty_emit = now + 2.0

    while not stop_event.wait(interval):
        active_start = _active_batch_start()

        if active_start is None:
            if current_batch_started_at is not None:
                final_progress = _snapshot(current_batch_started_at)
                _emit(final_progress, final=True)
                current_batch_started_at = None
                last_progress = None
                tick = 0
                last_len = 0
            continue

        if current_batch_started_at is None:
            current_batch_started_at = active_start
            _create_tqdm_bar()

        progress = _snapshot(current_batch_started_at)
        _emit(progress)
        last_progress = progress
        tick += 1

    if current_batch_started_at is not None:
        final_progress = _snapshot(current_batch_started_at)
        _emit(final_progress, final=True)
    else:
        _close_tqdm_bar()


def _render_initial_scan_target_line(entry: Dict[str, Any]) -> str:
    path = entry.get("path") or "-"
    volume_id = entry.get("uuid") or "-"
    return f"- {path} (volume={volume_id})"


def _render_initial_scan_result_line(result: Dict[str, Any]) -> str:
    path = result.get("path") or "-"
    volume_id = result.get("uuid") or "-"
    status = result.get("status") or "unknown"
    files_scanned = int(result.get("files_scanned") or 0)
    directories_seen = int(result.get("directories_seen") or 0)
    elapsed_value = result.get("elapsed_seconds")
    if isinstance(elapsed_value, (int, float)):
        elapsed = f"{float(elapsed_value):.1f}s"
    else:
        elapsed = "-"
    return (
        f"- {path} (volume={volume_id}) status={status}"
        f" files={files_scanned:,} dirs={directories_seen:,} elapsed={elapsed}"
    )


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
    polling_interval: Optional[int] = typer.Option(
        None,
        "--polling-interval",
        help="Polling interval in seconds when the inotify backend is unavailable (config key: run.polling_interval).",
        metavar="SECONDS",
    ),
    exclude: Optional[List[str]] = typer.Option(
        None,
        "--exclude",
        help="Glob-style path pattern to exclude from scans and live events (repeatable; config key: run.exclude_patterns).",
        metavar="PATTERN",
    ),
    scan_only: bool = typer.Option(
        False,
        "--scan-only",
        help="Perform the initial archival scan and exit without starting live watchers or auto-discovery.",
    ),
) -> None:
    """Start monitoring a directory (defaults to auto-detected mount points)."""
    logger = get_logger(__name__)

    perform_scan = _get_config_value("run.auto_scan")
    if scan_only:
        perform_scan = True
    elif scan is not None:
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

    polling_interval_cfg = _get_config_value("run.polling_interval")
    exclude_patterns_cfg = _get_config_value("run.exclude_patterns")

    effective_polling_interval = polling_interval if polling_interval is not None else polling_interval_cfg
    if effective_polling_interval is not None and effective_polling_interval < 1:
        raise typer.BadParameter("Polling interval must be at least 1 second.")

    if exclude:
        exclude_patterns = list(exclude)
    else:
        exclude_patterns = list(exclude_patterns_cfg) if exclude_patterns_cfg else []

    manager = DiskWatcherManager(
        polling_interval=effective_polling_interval,
        exclude_patterns=exclude_patterns,
    )

    # Prevent confusing "active" jobs from previous crashed runs.
    try:
        cleanup_stale_jobs(manager.conn, lock=manager.conn_lock)
    except Exception:
        logger.debug("stale_job_cleanup_failed", exc_info=True)

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

    if auto_roots and not scan_only:
        logger.info(
            "Auto-discovery roots configured",
            extra={"roots": [str(root) for root in auto_roots]},
        )

        manager.enable_auto_discovery(
            auto_roots,
            scan_new=False,
            max_workers=max_scan_workers,
            start_thread=False,
        )
        # Start discovery before the initial scan so newly mounted drives are
        # detected even while the archival sweep is running.
        manager.set_auto_discovery_scan_new(perform_scan)
        manager.start_auto_discovery_thread()

    progress_stop = Event()
    progress_thread: Optional[Thread] = None
    if perform_scan:
        progress_thread = Thread(
            target=_monitor_initial_scan_batches,
            args=(manager.conn, progress_stop, manager.conn_lock),
            kwargs={"owner_pid": str(os.getpid())},
            daemon=True,
        )
        progress_thread.start()

    if perform_scan:
        directory_count = len(manager.current_paths())
        parallel = directory_count > 1
        targets = manager.status()
        if targets:
            typer.echo("Initial scan targets:")
            for entry in targets:
                typer.echo(_render_initial_scan_target_line(entry))
        else:
            typer.echo("Initial scan targets: (none)")
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
        scan_results: List[Dict[str, Any]] = []
        try:
            scan_results = manager.run_initial_scans(parallel=parallel, max_workers=max_scan_workers)
        finally:
            pass
        if scan_results:
            typer.echo("Initial scan results:")
            for result in scan_results:
                typer.echo(_render_initial_scan_result_line(result))
    else:
        logger.info("Skipping initial archival scan (run.auto_scan disabled).")

    if scan_only:
        logger.info("Scan-only mode enabled; exiting after archival sweep.")
        progress_stop.set()
        if progress_thread is not None:
            progress_thread.join(timeout=2.0)
        manager.stop_all()
        return

    manager.start_all()

    if auto_roots:
        manager.set_auto_discovery_scan_new(perform_scan)
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
        pass
    finally:
        progress_stop.set()
        if progress_thread is not None:
            progress_thread.join(timeout=2.0)
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


@app.command()
def web(
    host: str = typer.Option("127.0.0.1", help="Interface to bind the dashboard server."),
    port: int = typer.Option(5000, help="Port to run the dashboard server."),
    refresh: int = typer.Option(5, help="Seconds between dashboard auto-refresh."),
    event_limit: int = typer.Option(25, help="Number of recent events to display."),
) -> None:
    """Run a small web dashboard showing live jobs, volumes, and events."""

    try:
        from diskwatcher.web import create_app
    except ImportError as exc:  # pragma: no cover - defensive guard
        typer.echo(f"Flask is required for the dashboard: {exc}")
        raise typer.Exit(code=1) from exc

    typer.echo(
        f"Starting dashboard on http://{host}:{port} (refresh {refresh}s, events {event_limit})"
    )
    app = create_app(refresh_seconds=refresh, event_limit=event_limit)
    try:
        app.run(host=host, port=port)
    except KeyboardInterrupt:
        typer.echo("Stopping dashboard...")


@config_app.command("show")
def config_show(
    as_json: bool = typer.Option(False, "--json", help="Emit configuration as JSON."),
) -> None:
    """Display the effective configuration values and their defaults."""

    try:
        data = config_utils.list_config()
    except config_utils.ConfigError as exc:
        _emit_config_error(exc)

    active_log_dir_value = active_log_dir()
    active_log_file_value = active_log_file()

    storage_paths = {
        "config_dir": str(config_utils.config_dir()),
        "config_file": str(config_utils.config_path()),
        "database_dir": str(DB_DIR),
        "database_file": str(DB_PATH),
        "log_dir": str(LOG_DIR),
        "log_file": str(LOG_FILE),
        "log_dir_active": str(active_log_dir_value) if active_log_dir_value else None,
        "log_file_active": str(active_log_file_value) if active_log_file_value else None,
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

    candidates = []
    active_path = active_log_file()
    if active_path is not None:
        candidates.append(active_path)
    if LOG_FILE not in candidates:
        candidates.append(LOG_FILE)

    for candidate in candidates:
        try:
            if candidate.exists():
                typer.echo(candidate.read_text())
                return
        except OSError as exc:
            typer.echo(f"Unable to read log file {candidate}: {exc}", err=True)

    typer.echo("No logs found.")


def _write_labels_csv(path: Path, columns: List[str], rows: List[Dict[str, Any]]) -> None:
    import csv

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([row.get(column, "") for column in columns])


def _write_labels_xlsx(path: Path, columns: List[str], rows: List[Dict[str, Any]]) -> None:
    try:
        from openpyxl import Workbook  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - defensive guard
        raise RuntimeError(
            "openpyxl is required for XLSX export; install it with "
            "'python -m pip install openpyxl' or use --format csv."
        ) from exc

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Volumes"
    sheet.append(columns)
    for row in rows:
        sheet.append([row.get(column, "") for column in columns])
    workbook.save(path)


@app.command()
def labels(
    output: Path = typer.Argument(
        ...,
        exists=False,
        dir_okay=False,
        file_okay=True,
        resolve_path=True,
        help="Output .xlsx or .csv file for label printing.",
    ),
    fmt: Optional[str] = typer.Option(
        None,
        "--format",
        "-f",
        help="Output format: xlsx or csv (inferred from file extension when omitted).",
    ),
) -> None:
    """Export tracked volumes to a spreadsheet suitable for label printers."""

    try:
        with init_db() as conn:
            ensure_volume_label_indices(conn)
            records = fetch_volume_metadata(conn)
    except sqlite3.OperationalError:
        typer.echo("Catalog is empty. Run `diskwatcher run` to start logging events.")
        raise typer.Exit(code=1)

    if not records:
        typer.echo("No volumes recorded yet.")
        raise typer.Exit(code=0)

    rows = build_label_rows(records)
    remaining_columns = [column for column in LABEL_EXPORT_COLUMNS if column != "mount_label"]
    columns = ["label_index", "mount_label", "human_id"] + remaining_columns

    target_format = (fmt or "").lower()
    if not target_format:
        suffix = output.suffix.lower()
        if suffix == ".csv":
            target_format = "csv"
        else:
            target_format = "xlsx"

    output_path = output
    if not output_path.suffix:
        output_path = output_path.with_suffix(f".{target_format}")

    try:
        if target_format == "csv":
            _write_labels_csv(output_path, columns, rows)
        elif target_format == "xlsx":
            _write_labels_xlsx(output_path, columns, rows)
        else:
            raise typer.BadParameter("Unsupported format. Choose 'csv' or 'xlsx'.")
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1)

    typer.echo(f"Wrote {len(rows)} volume labels to {output_path} ({target_format}).")


@app.command()
def status(
    limit: int = typer.Option(10, help="Number of recent events to display."),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON instead of text."),
) -> None:
    """Show a snapshot of recent catalog activity."""
    try:
        with init_db() as conn:
            cleanup_stale_jobs(conn)
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
    basename: bool,
) -> List[Dict[str, Any]]:
    params: List[Any] = []
    column = "dw_basename(path)" if basename else "path"
    clause = _build_search_clause(
        column,
        pattern,
        regex=regex,
        case_sensitive=case_sensitive,
        params=params,
    )

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
    # Matching is delegated to the dw_match_pattern SQLite function,
    # which is configured per-search with the chosen substring/regex
    # semantics and case-sensitivity. The params list is unused here
    # but kept for signature compatibility.
    return f"dw_match_pattern({column})"


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
    case_sensitive: bool = typer.Option(
        True,
        "--case-sensitive/--ignore-case",
        help="Match with case sensitivity (use --ignore-case for case-insensitive matching).",
    ),
    ignore_case_short: bool = typer.Option(
        False,
        "-i",
        help="Shortcut for --ignore-case (case-insensitive matching, similar to find -i).",
    ),
    iname: bool = typer.Option(
        False,
        "--iname",
        help="Alias for case-insensitive basename search (like find -iname).",
    ),
    basename: bool = typer.Option(
        True,
        "--basename/--no-basename",
        help="Match file basenames by default; disable to search full paths.",
    ),
    wholename: bool = typer.Option(
        False,
        "--wholename",
        help="Match against full stored paths (alias for --no-basename, similar to find -wholename).",
    ),
    include_deleted: bool = typer.Option(False, "--include-deleted", help="Include deleted files in results."),
    limit: int = typer.Option(50, help="Maximum rows per section."),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON payload instead of text."),
) -> None:
    """Search the catalog for files and/or directories."""

    if not files and not directories:
        raise typer.BadParameter("Enable files and/or directories to search.")

    # Apply user-friendly aliases.
    if ignore_case_short:
        case_sensitive = False
    if iname:
        case_sensitive = False
        basename = True
    if wholename:
        basename = False

    try:
        with init_db() as conn:
            conn.row_factory = sqlite3.Row

            if regex:
                flags = 0 if case_sensitive else re.IGNORECASE
                try:
                    compiled_re = re.compile(pattern, flags)
                except re.error as exc:
                    raise typer.BadParameter(f"Invalid regular expression: {exc}") from exc

                def _match(value: Optional[str]) -> int:
                    return int(bool(value and compiled_re.search(value)))
            else:
                if case_sensitive:

                    def _match(value: Optional[str]) -> int:
                        return int(bool(value and pattern in value))
                else:
                    lowered = pattern.lower()

                    def _match(value: Optional[str]) -> int:
                        return int(bool(value and lowered in value.lower()))

            conn.create_function("dw_match_pattern", 1, _match)

            if basename:

                def _basename(value: Optional[str]) -> Optional[str]:
                    if value is None:
                        return None
                    try:
                        return Path(value).name
                    except Exception:
                        return value

                conn.create_function("dw_basename", 1, _basename)

            file_results = []
            dir_results = []

            if files:
                file_results = _search_files(
                    conn,
                    pattern,
                    regex=regex,
                    case_sensitive=case_sensitive,
                    include_deleted=include_deleted,
                    limit=limit,
                    basename=basename,
                )

            if directories:
                dir_results = _search_directories(
                    conn,
                    pattern,
                    regex=regex,
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
