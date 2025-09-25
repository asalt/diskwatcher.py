import json
import logging
import sys
import time
import sqlite3
from pathlib import Path
from typing import Any, List, Optional
from urllib.parse import urlparse, unquote

import typer

from diskwatcher.core.manager import DiskWatcherManager
from diskwatcher.core.inspector import suggest_directories
from diskwatcher.utils import config as config_utils
from diskwatcher.utils.logging import setup_logging, get_logger, LOG_DIR, LOG_FILE
from diskwatcher.db import init_db, query_events
from diskwatcher.db.connection import DB_PATH, DB_DIR
from diskwatcher.db.events import summarize_by_volume, summarize_files, query_events_since
from diskwatcher.db.migration import upgrade as migrate_upgrade, build_alembic_config


_LOG_LEVEL_CHOICES = {
    name: getattr(logging, name.upper()) for name in config_utils.LOG_LEVEL_VALUES
}
_LOG_LEVEL_CHOICES["warn"] = logging.WARNING


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
) -> None:
    """Start monitoring a directory (defaults to auto-detected mount points)."""
    logger = get_logger(__name__)

    perform_scan = _get_config_value("run.auto_scan")
    if scan is not None:
        perform_scan = scan

    if not directories:
        suggested = suggest_directories()
        if not suggested:
            logger.error(
                "No suitable directories found to monitor. Specify one manually."
            )
            return
        directories = suggested

    directories = [Path(d).resolve() for d in directories]
    manager = DiskWatcherManager()
    for directory in directories:
        manager.add_directory(Path(directory))

    if perform_scan:
        logger.info("Performing initial archival scan of monitored directories.")
        for thread in manager.threads:
            thread.watcher.archive_existing_files()
    else:
        logger.info("Skipping initial archival scan (run.auto_scan disabled).")

    manager.start_all()

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
    except sqlite3.OperationalError:
        typer.echo("Catalog is empty. Run `diskwatcher run` to start logging events.")
        return

    if as_json:
        payload = {"events": events, "volumes": aggregates}
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

    if aggregates:
        typer.echo("\nBy volume:")
        for agg in aggregates:
            typer.echo(
                f"{agg['volume_id']} @ {agg['directory']} => total={agg['total_events']}"
                f" (created={agg['created']}, modified={agg['modified']}, deleted={agg['deleted']})"
            )


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
        for d in suggested_dirs:
            typer.echo(f"  - {d}")


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
