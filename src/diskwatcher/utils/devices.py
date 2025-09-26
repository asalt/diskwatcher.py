import platform
import re
import subprocess
from pathlib import Path
from typing import Dict, Optional

DEFAULT_TIMEOUT_SECONDS = 5.0

LSBLK_FIELDS: tuple[str, ...] = (
    "NAME",
    "PATH",
    "MOUNTPOINT",
    "MAJ:MIN",
    "UUID",
    "LABEL",
    "PTUUID",
    "PTTYPE",
    "PARTTYPENAME",
    "PARTTYPE",
    "PARTUUID",
    "SIZE",
    "MODEL",
    "SERIAL",
    "VENDOR",
    "FSVER",
    "WWN",
)

IDENTIFIER_COMPONENTS: tuple[str, ...] = (
    "UUID",
    "PARTUUID",
    "PTUUID",
    "WWN",
    "SERIAL",
    "MODEL",
    "VENDOR",
    "FSVER",
)


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
    fallback_device = anchor or str(resolved)
    return {
        "directory": str(resolved),
        "mount_point": anchor or str(resolved),
        "device": fallback_device,
        "volume_id": fallback_device,
        "uuid": None,
        "label": None,
        "lsblk": None,
    }


def _build_volume_identifier(fields: Dict[str, Optional[str]], fallback: str) -> str:
    """Coalesce a stable identifier from lsblk metadata."""

    components = []
    for key in IDENTIFIER_COMPONENTS:
        value = fields.get(key)
        if value:
            components.append(f"{key.lower()}={value}")

    if components:
        return "|".join(components)

    for key in ("PATH", "NAME"):
        value = fields.get(key)
        if value:
            return value

    maj_min = fields.get("MAJ:MIN")
    if maj_min:
        return f"maj:min={maj_min}"

    return fallback


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
    volume_id = None
    lsblk_details: Optional[Dict[str, Optional[str]]] = None

    try:
        lsblk_output = _run_command(["lsblk", "-P", "-o", ",".join(LSBLK_FIELDS)])

        for line in lsblk_output.splitlines():
            parsed = parse_lsblk_line(line)
            fields = {
                k: (v.strip() if v and v.strip() else None) for k, v in parsed.items()
            }
            name = fields.get("NAME")
            path_or_device = fields.get("PATH")
            if name == device_name or path_or_device == device:
                uuid = fields.get("UUID")
                label = fields.get("LABEL")
                volume_id = _build_volume_identifier(fields, device)
                lsblk_details = fields
                break
    except RuntimeError:
        pass

    return {
        "directory": str(path),
        "mount_point": mount_point or str(path),
        "device": device or str(path),
        "volume_id": volume_id or uuid or label or device or str(path),
        "uuid": uuid or None,
        "label": label or None,
        "lsblk": lsblk_details,
    }
