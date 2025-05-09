import click
import logging
import time
from pathlib import Path
from diskwatcher.core.watcher import DiskWatcher
from diskwatcher.core.manager import DiskWatcherManager
from diskwatcher.core.inspector import suggest_directories
from diskwatcher.utils.logging import setup_logging, get_logger
from diskwatcher.utils.devices import get_mount_info


@click.group()
@click.option(
    "--log-level",
    default="info",
    help="Set logging level (debug, info, warning, error)",
)
def main(log_level):
    """DiskWatcher CLI - Monitor filesystem events"""
    log_level = getattr(logging, log_level.upper(), logging.INFO)
    setup_logging(level=log_level)


@main.command()
@click.argument("directories", nargs=-1, type=click.Path(exists=True))
@click.option(
    "--no-scan",
    is_flag=True,
    default=False,
    show_default=True,
    help="Skip initial archival scan of existing files",
)
def run(no_scan, directories):
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
    try:
        while True:
            counter = 0
            time.sleep(1)
            if counter % 10 == 0:
                status = manager.status()
                logger.debug(f"Current status: {status}")
            counter += 1
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


@main.command()
def log():
    """Show recent log entries"""
    log_file = Path.home() / ".diskwatcher/diskwatcher.log"
    if log_file.exists():
        click.echo(log_file.read_text())
    else:
        click.echo("No logs found.")


@main.command()
def status():
    """show status"""
    pass


@main.command()
def suggest():
    """Inspect system and suggest directories to monitor."""
    from diskwatcher.core.inspector import suggest_directories

    suggested_dirs = suggest_directories()
    if not suggested_dirs:
        click.echo("No suitable directories found.")
    else:
        click.echo("Suggested directories to monitor:")
        for d in suggested_dirs:
            click.echo(f"  - {d}")


# Attach subcommands  not necessary with main.command
# main.add_command(run)
# main.add_command(log)

if __name__ == "__main__":
    main()
