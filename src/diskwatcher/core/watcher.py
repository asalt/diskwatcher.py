from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
from diskwatcher.utils.logging import get_logger
from pathlib import Path
from threading import Event
from typing import Optional

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = get_logger(__name__)

class DiskWatcher(FileSystemEventHandler):
    """Watches for file system changes in a given directory."""

    def __init__(self, path: str, uuid: str = None):
        self.path = Path(path)
        if uuid is None:
            from diskwatcher.utils.devices import get_mount_info
            possible_uuid = get_mount_info(path)
            uuid = possible_uuid['uuid'] or possible_uuid['label'] or possible_uuid['device']
        self.uuid = uuid 

    def on_modified(self, event):
        logger.info(f"File modified: {event.src_path}")

    def on_created(self, event):
        logger.info(f"File created: {event.src_path}")

    def on_deleted(self, event):
        logger.info(f"File deleted: {event.src_path}")

    def start(self, recursive=True, run_once=False, stop_event: Optional[Event]=None):

        if stop_event is not None and not isinstance(stop_event, Event):
            raise TypeError("stop_event must be a threading.Event or None")


        observer = Observer()
        observer.schedule(self, self.path, recursive=recursive)

        logger.info(f"Watching {self.uuid} : {self.path}...")
        observer.start()

        try:
            while True:
                time.sleep(1)
                if run_once or (stop_event and stop_event.is_set()):
                    break
        finally:
            observer.stop()
            observer.join()

