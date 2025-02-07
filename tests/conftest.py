import pytest
import logging
from pathlib import Path

@pytest.fixture
def fake_mounts(tmp_path):
    """Create a fake /proc/mounts file for testing."""
    mounts_file = tmp_path / "proc_mounts"
    mounts_file.write_text(
        "/dev/sda1 /mnt ext4 rw,relatime 0 0\n"
        "/dev/sdb1 /media/user/usb ext4 rw,relatime 0 0\n"
    )
    return mounts_file


@pytest.fixture(scope="session", autouse=True)
def setup_logging():
    """Enable logging for tests"""
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

