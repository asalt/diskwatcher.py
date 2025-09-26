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
    assert info["volume_id"] == info["device"]
    assert info["lsblk"] is None


@patch("diskwatcher.utils.devices.platform.system", return_value="Linux")
@patch("diskwatcher.utils.devices._run_command", side_effect=RuntimeError("boom"))
def test_get_mount_info_falls_back_when_commands_fail(mock_run, mock_platform, sample_path):
    info = devices.get_mount_info(sample_path)
    resolved = sample_path.resolve()
    assert info["directory"] == str(resolved)
    assert info["mount_point"].startswith(resolved.anchor or str(resolved))
    assert info["uuid"] is None
    assert info["label"] is None
    assert info["volume_id"] == info["device"]
    assert info["lsblk"] is None


@patch("diskwatcher.utils.devices.platform.system", return_value="Linux")
def test_get_mount_info_aggregates_lsblk_metadata(mock_platform, sample_path):
    mount_point = "/mnt/demo"
    device_path = "/dev/sdb2"

    lsblk_lines = [
        "NAME=\"sdb\" PATH=\"/dev/sdb\" MOUNTPOINT=\"\" MAJ:MIN=\"8:16\" UUID=\"\" LABEL=\"\" PTUUID=\"7633aa30-9e04-4103-be0a-c4c094450868\" PTTYPE=\"gpt\" PARTTYPENAME=\"\" PARTTYPE=\"\" PARTUUID=\"\" SIZE=\"4.5T\" MODEL=\"BUP Portable\" SERIAL=\"00000000NAB9X7PR\" VENDOR=\"Seagate \" FSVER=\"\" WWN=\"\"",
        "NAME=\"sdb2\" PATH=\"/dev/sdb2\" MOUNTPOINT=\"/mnt/demo\" MAJ:MIN=\"8:18\" UUID=\"5DEA-8F0F\" LABEL=\"Archive\" PTUUID=\"7633aa30-9e04-4103-be0a-c4c094450868\" PTTYPE=\"gpt\" PARTTYPENAME=\"Microsoft basic data\" PARTTYPE=\"ebd0a0a2-b9e5-4433-87c0-68b6b72699c7\" PARTUUID=\"72285562-76ab-40d7-82e6-c12171690add\" SIZE=\"4.5T\" MODEL=\"\" SERIAL=\"\" VENDOR=\"\" FSVER=\"1.0\" WWN=\"\"",
    ]

    with patch(
        "diskwatcher.utils.devices._run_command",
        side_effect=[mount_point, device_path, "\n".join(lsblk_lines)],
    ):
        info = devices.get_mount_info(mount_point)

    assert info["device"] == device_path
    assert info["uuid"] == "5DEA-8F0F"
    assert info["label"] == "Archive"
    assert info["mount_point"] == mount_point
    assert info["volume_id"].startswith("uuid=5DEA-8F0F")
    assert "partuuid=72285562-76ab-40d7-82e6-c12171690add" in info["volume_id"]
    assert info["lsblk"]["PTUUID"] == "7633aa30-9e04-4103-be0a-c4c094450868"
    assert info["lsblk"]["PARTUUID"] == "72285562-76ab-40d7-82e6-c12171690add"
    assert info["lsblk"]["FSVER"] == "1.0"
