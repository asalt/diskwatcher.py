import click
import logging
from pathlib import Path
from diskwatcher.core.watcher import DiskWatcher
from diskwatcher.core.inspector import suggest_directories
from diskwatcher.utils.logging import setup_logging, get_logger
from diskwatcher.utils.devices import get_mount_info

@click.group()
@click.option("--log-level", default="info", help="Set logging level (debug, info, warning, error)")
def main(log_level):
    """DiskWatcher CLI - Monitor filesystem events"""
    log_level = getattr(logging, log_level.upper(), logging.INFO)
    setup_logging(level=log_level)


@main.command()
@click.argument("directory", type=click.Path(exists=True), required=False)
def run(directory):
    """Start monitoring a directory (defaults to auto-detected mount points)."""
    logger = get_logger(__name__)

    if not directory:
        suggested = suggest_directories()
        if not suggested:
            logger.error("No suitable directories found to monitor. Specify one manually.")
            return
        directory = suggested[0]  # Default to first suggestion

    directory = Path(directory).resolve()


    uuid = None
    try:
        info = get_mount_info(directory)
        uuid = info["uuid"] or info["label"] or info["device"]
    except Exception as e:
        logger.warning(f"Could not resolve volume UUID: {e}")
        uuid = directory

    logger.info(f"Starting DiskWatcher on {directory}")
    watcher = DiskWatcher(directory, uuid=uuid)
    watcher.start()

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

