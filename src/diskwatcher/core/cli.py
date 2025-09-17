import json
import logging
import time
import sqlite3
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse, unquote

import typer

from diskwatcher.core.manager import DiskWatcherManager
from diskwatcher.core.inspector import suggest_directories
from diskwatcher.utils.logging import setup_logging, get_logger
from diskwatcher.db import init_db, query_events
from diskwatcher.db.connection import DB_PATH
from diskwatcher.db.events import summarize_by_volume
from diskwatcher.db.migration import upgrade as migrate_upgrade, build_alembic_config


app = typer.Typer(help="DiskWatcher CLI - Monitor filesystem events.", no_args_is_help=True)
dev_app = typer.Typer(help="Developer tooling for migrations and catalog upkeep.")
app.add_typer(dev_app, name="dev")


@app.callback()
def main(log_level: str = typer.Option("info", help="Logging level (debug, info, warning, error)")) -> None:
    """Entry point for the DiskWatcher CLI."""
    resolved_level = getattr(logging, log_level.upper(), logging.INFO)
    setup_logging(level=resolved_level)


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
    no_scan: bool = typer.Option(
        False,
        "--no-scan",
        help="Skip initial archival scan of existing files.",
    ),
) -> None:
    """Start monitoring a directory (defaults to auto-detected mount points)."""
    logger = get_logger(__name__)

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


@app.command()
def log() -> None:
    """Show recent log entries"""
    log_file = Path.home() / ".diskwatcher/diskwatcher.log"
    if log_file.exists():
        click.echo(log_file.read_text())
    else:
        click.echo("No logs found.")


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


def entrypoint() -> None:
    """Console script entrypoint."""

    app()


if __name__ == "__main__":
    entrypoint()
