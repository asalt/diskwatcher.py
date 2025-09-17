import platform
import re
import subprocess
from pathlib import Path
from typing import Dict, Optional

DEFAULT_TIMEOUT_SECONDS = 5.0


def parse_lsblk_line(line: str) -> dict:
    """Parse a single line of lsblk -P output into a dictionary."""
    return dict(re.findall(r'(\w+)="(.*?)"', line))


def _run_command(args: list[str]) -> str:
    """Return stdout for a command or raise a RuntimeError with context."""
    try:
        completed = subprocess.run(
            args,
            check=True,
            text=True,
            capture_output=True,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"Required command not found: {args[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"Command timed out: {' '.join(args)}") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"Command failed: {' '.join(args)} (stderr: {exc.stderr.strip()})"
        ) from exc
    return completed.stdout.strip()


def _fallback_mount_info(directory: Path) -> Dict[str, Optional[str]]:
    resolved = directory.resolve()
    anchor = resolved.anchor or str(resolved.root)
    return {
        "directory": str(resolved),
        "mount_point": anchor or str(resolved),
        "device": anchor or str(resolved),
        "uuid": None,
        "label": None,
    }


def get_mount_info(directory: str) -> dict:
    """Return mount metadata for the supplied directory.

    Falls back to best-effort values when platform utilities are unavailable.
    """

    path = Path(directory).resolve()

    if platform.system() != "Linux":
        return _fallback_mount_info(path)

    try:
        mount_point = _run_command(
            ["findmnt", "--noheadings", "--output", "TARGET", "--target", str(path)]
        )
        device = _run_command(
            ["findmnt", "--noheadings", "--output", "SOURCE", "--target", mount_point]
        )
    except RuntimeError:
        return _fallback_mount_info(path)

    device_name = Path(device).name
    uuid = None
    label = None

    try:
        lsblk_output = _run_command(["lsblk", "-P", "-o", "NAME,UUID,LABEL"])
        for line in lsblk_output.splitlines():
            fields = parse_lsblk_line(line)
            if fields.get("NAME") == device_name:
                uuid = fields.get("UUID")
                label = fields.get("LABEL")
                break
    except RuntimeError:
        pass

    return {
        "directory": str(path),
        "mount_point": mount_point or str(path),
        "device": device or str(path),
        "uuid": uuid or None,
        "label": label or None,
    }
