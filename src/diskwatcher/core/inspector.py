import os
import platform
import logging
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)

DEFAULT_LINUX_DIRS = ["/mnt", "/media", "/run/media"]

def get_mount_points() -> List[Path]:
    """Detect mounted filesystems (Linux-specific for now)."""
    if platform.system() != "Linux":
        logger.warning("Mount point detection is currently only supported on Linux.")
        return []

    mount_points = []
    try:
        with open("/proc/mounts", "r") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    mount_dir = Path(parts[1])
                    if any(mount_dir.parts[:2] == Path(d).parts for d in DEFAULT_LINUX_DIRS):
                        mount_points.append(mount_dir)
    except Exception as e:
        logger.error(f"Error reading /proc/mounts: {e}")

    return mount_points

def suggest_directories() -> List[Path]:
    """Suggest directories to monitor (defaulting to Linux-specific paths)."""
    mounts = get_mount_points()
    if mounts:
        return mounts
    return [Path(d) for d in DEFAULT_LINUX_DIRS if Path(d).exists()]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    suggested_dirs = suggest_directories()
    print("Suggested directories to monitor:")
    for d in suggested_dirs:
        print(f"  - {d}")

