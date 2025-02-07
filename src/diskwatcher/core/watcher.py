from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

class DiskWatcher(FileSystemEventHandler):
    """Watches for file system changes in a given directory."""

    def __init__(self, path: str):
        self.path = Path(path)

    def on_modified(self, event):
        logging.info(f"File modified: {event.src_path}")

    def on_created(self, event):
        logging.info(f"File created: {event.src_path}")

    def on_deleted(self, event):
        logging.info(f"File deleted: {event.src_path}")

    def start(self):
        observer = Observer()
        observer.schedule(self, self.path, recursive=True)
        observer.start()
        logging.info(f"Watching {self.path}...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()

