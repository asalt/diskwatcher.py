import subprocess
from pathlib import Path
from typing import Optional

import re
from snoop import snoop

def parse_lsblk_line(line: str) -> dict:
    """Parse a single line of lsblk -P output into a dictionary."""
    return dict(re.findall(r'(\w+)="(.*?)"', line))

# @snoop
def get_mount_info(directory: str) -> dict:
    """Given a directory, return mount info including device, UUID (if any), and volume label."""
    directory = str(Path(directory).resolve())

    # Step 1: find mount point and device
    try:
        mount_point = subprocess.check_output(
            ['findmnt', '--noheadings', '--output', 'TARGET', '--target', directory],
            text=True
        ).strip()

        device = subprocess.check_output(
            ['findmnt', '--noheadings', '--output', 'SOURCE', '--target', mount_point],
            text=True
        ).strip()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to resolve mount point for {directory}: {e}")

    device_name = Path(device).name
    uuid = None
    label = None

    try:
        lsblk_output = subprocess.check_output(
            ['lsblk', '-P', '-o', 'NAME,UUID,LABEL'],
            text=True
        )
        for line in lsblk_output.strip().splitlines():
            fields = parse_lsblk_line(line)
            if fields.get("NAME") == device_name:
                uuid = fields.get("UUID")
                label = fields.get("LABEL")
                break
    except subprocess.CalledProcessError:
        pass

    return {
        "directory": directory,
        "mount_point": mount_point,
        "device": device,
        "uuid": uuid or None,
        "label": label or None,
    }

