import pytest
import logging
from unittest.mock import patch
from pathlib import Path
from diskwatcher.core.inspector import get_mount_points, suggest_directories

logger = logging.getLogger(__name__)


@pytest.fixture
def fake_directories(tmp_path):
    """Create a fake directory structure for testing suggest_directories."""
    fake_mnt = tmp_path / "mnt"
    fake_media = tmp_path / "media"
    fake_mnt.mkdir()
    fake_media.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Created fake directories: {fake_mnt}, {fake_media}")


    return [fake_mnt, fake_media]

@patch("builtins.open")
def test_get_mount_points(mock_open, fake_mounts):
    """Test mount point detection from a fake /proc/mounts."""
    mock_open.return_value.__enter__.return_value = fake_mounts.open()
    mounts = get_mount_points()

    assert Path("/mnt") in mounts
    assert Path("/media/user/usb") in mounts
    assert len(mounts) == 2  # Ensure both mount points are detected


@patch("diskwatcher.core.inspector.DEFAULT_LINUX_DIRS", new=["/mnt", "/media"])
@patch("diskwatcher.core.inspector.get_mount_points")
@patch("pathlib.Path.exists")
def test_suggest_directories(mock_exists, mock_get_mounts, fake_directories):
    """Test that suggest_directories returns the fake directories."""

    # Ensure `Path.exists()` only returns True for our fake directories
    def fake_exists(path):
        return Path(path) in fake_directories

    mock_exists.side_effect = fake_exists

    # Mock `get_mount_points()` to return our fake directories
    mock_get_mounts.return_value = fake_directories

    logger.debug("Calling suggest_directories()...")
    suggested = suggest_directories()
    logger.debug(f"Suggested directories: {suggested}")


    assert fake_directories[0] in suggested  # /mnt
    assert fake_directories[1] in suggested  # /media
    assert len(suggested) == 2  # Ensure both directories are found

