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


def pytest_addoption(parser):
    print("DEBUG: pytest_addoption() called!")  # Ensure this prints

    parser.addoption("--log-debug", action="store_true", help="Enable debug logging")


@pytest.fixture(scope="session", autouse=True)
def setup_logging(request):
    """Ensure debug logging is correctly applied."""
    log_debug_enabled = request.config.getoption("--log-debug", default=False)
    log_level = logging.DEBUG if log_debug_enabled else logging.INFO

    # Reset any existing handlers (important for pytest)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Configure logging again
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("logs/test.log")],
    )

    logging.debug(
        "DEBUG: Logging setup complete!"
    )  # Should print if --log-debug is active
