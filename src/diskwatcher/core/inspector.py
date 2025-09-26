import os
import platform
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List

from diskwatcher.utils.devices import get_mount_info

logger = logging.getLogger(__name__)

DEFAULT_LINUX_DIRS = ["/mnt", "/media", "/run/media"]


@dataclass(frozen=True)
class DirectorySuggestion:
    path: Path
    volume_id: str

    def __eq__(self, other: object) -> bool:  # pragma: no cover - exercised via tests.
        if isinstance(other, DirectorySuggestion):
            return (self.path, self.volume_id) == (other.path, other.volume_id)
        if isinstance(other, Path):
            return self.path == other
        return NotImplemented

    def __hash__(self) -> int:  # pragma: no cover - structural helper.
        return hash((self.path, self.volume_id))


def _coalesce_volume_id(info: dict, fallback: str) -> str:
    for key in ("volume_id", "uuid", "label", "device"):
        value = info.get(key)
        if value:
            return value
    return fallback


def _resolve_volume_id(path: Path) -> str:
    try:
        info = get_mount_info(path)
    except Exception as exc:  # pragma: no cover - defensive logging guard.
        logger.warning("Failed to look up mount info", extra={"path": str(path), "error": str(exc)})
        return str(path)
    return _coalesce_volume_id(info, str(path))


def get_mount_points_unix() -> List[Path]:
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
                    if any(
                        mount_dir.parts[:2] == Path(d).parts for d in DEFAULT_LINUX_DIRS
                    ):
                        logger.info(f"Adding {mount_dir} to mount points")
                        mount_points.append(mount_dir)
    except Exception as e:
        logger.error(f"Error reading /proc/mounts: {e}")

    return mount_points


def get_mount_points() -> List[Path]:
    """Get mount points based on the operating system."""
    if platform.system() == "Linux":
        return get_mount_points_unix()
    else:
        logger.warning("Mount point detection is not implemented for this OS.")
        return []


def suggest_directories() -> List[DirectorySuggestion]:
    """Suggest directories to monitor along with unique volume identifiers."""
    mounts = get_mount_points()
    if mounts:
        candidates = mounts
    else:
        candidates = [Path(d) for d in DEFAULT_LINUX_DIRS if Path(d).exists()]

    suggestions: List[DirectorySuggestion] = []
    for directory in candidates:
        resolved = directory.resolve()
        try:
            if not Path.exists(resolved):
                continue
            volume_id = _resolve_volume_id(resolved)
            suggestions.append(DirectorySuggestion(path=resolved, volume_id=volume_id))
        except PermissionError as e:
            logger.warning(f"Permission error for {volume_id}")
    return suggestions


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    suggested_dirs = suggest_directories()
    print("Suggested directories to monitor:")
    for suggestion in suggested_dirs:
        print(f"  - {suggestion.path} (volume_id={suggestion.volume_id})")
