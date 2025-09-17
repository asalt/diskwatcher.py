import logging
from datetime import datetime
from pathlib import Path

import pytest


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
    parser.addoption("--log-debug", action="store_true", help="Enable debug logging")
    parser.addoption(
        "--integration",
        action="store_true",
        help="Run integration tests that exercise watcher threads.",
    )
    parser.addoption(
        "--keep-artifacts",
        action="store_true",
        help="Persist test artifacts under logs/artifacts for inspection.",
    )
    parser.addoption(
        "--artifact-dir",
        action="store",
        default=None,
        help="Custom base directory for storing test artifacts.",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: mark tests that require --integration to run"
    )


def pytest_runtest_setup(item):
    if "integration" in item.keywords and not item.config.getoption("--integration"):
        pytest.skip("integration tests require --integration flag")


@pytest.fixture(scope="session", autouse=True)
def setup_logging(request):
    """Ensure debug logging is correctly applied."""
    log_debug_enabled = request.config.getoption("--log-debug", default=False)
    log_level = logging.DEBUG if log_debug_enabled else logging.INFO

    artifact_root_option = request.config.getoption("--artifact-dir")
    if artifact_root_option:
        log_file_path = Path(artifact_root_option).expanduser().resolve() / "pytest.log"
    elif request.config.getoption("--keep-artifacts"):
        persistent_root = Path("logs") / "artifacts"
        persistent_root.mkdir(parents=True, exist_ok=True)
        log_file_path = persistent_root / "pytest.log"
    else:
        log_file_path = Path("logs") / "test.log"

    log_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Reset any existing handlers (important for pytest)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Configure logging again
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_file_path)],
    )

    logging.debug(
        "DEBUG: Logging setup complete!"
    )  # Should print if --log-debug is active


@pytest.fixture
def artifact_dir(request, tmp_path_factory):
    """Return a directory for test artifacts, optionally persisted between runs."""

    base_dir_option = request.config.getoption("--artifact-dir")
    keep_flag = request.config.getoption("--keep-artifacts")

    if base_dir_option:
        base_path = Path(base_dir_option).expanduser().resolve()
        base_path.mkdir(parents=True, exist_ok=True)
    elif keep_flag:
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        base_path = Path("logs") / "artifacts" / timestamp
        base_path.mkdir(parents=True, exist_ok=True)
    else:
        base_path = tmp_path_factory.mktemp("artifacts")

    node_name = request.node.name
    safe_name = "".join(
        c if c.isalnum() or c in ("-", "_") else "_" for c in node_name
    )
    artifact_path = base_path / safe_name
    artifact_path.mkdir(parents=True, exist_ok=True)
    return artifact_path
