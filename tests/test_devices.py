import platform
from pathlib import Path
from unittest.mock import patch

import pytest

from diskwatcher.utils import devices


@pytest.fixture
def sample_path(tmp_path):
    target = tmp_path / "nested"
    target.mkdir()
    return target


def test_get_mount_info_non_linux(sample_path):
    with patch.object(platform, "system", return_value="Darwin"):
        info = devices.get_mount_info(sample_path)
    resolved = sample_path.resolve()
    assert info["directory"] == str(resolved)
    assert info["mount_point"].startswith(resolved.anchor or str(resolved))
    assert info["device"]


@patch("diskwatcher.utils.devices.platform.system", return_value="Linux")
@patch("diskwatcher.utils.devices._run_command", side_effect=RuntimeError("boom"))
def test_get_mount_info_falls_back_when_commands_fail(mock_run, mock_platform, sample_path):
    info = devices.get_mount_info(sample_path)
    resolved = sample_path.resolve()
    assert info["directory"] == str(resolved)
    assert info["mount_point"].startswith(resolved.anchor or str(resolved))
    assert info["uuid"] is None
    assert info["label"] is None
