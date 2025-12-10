"""Helpers for deriving human-friendly IDs and export rows for volumes."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


LABEL_EXPORT_COLUMNS: List[str] = [
    "volume_id",
    "directory",
    "mount_label",
    "mount_uuid",
    "mount_volume_id",
    "mount_device",
    "lsblk_ptuuid",
    "lsblk_partuuid",
    "lsblk_wwn",
    "lsblk_model",
    "lsblk_serial",
    "lsblk_vendor",
    "lsblk_size",
    "usage_total_bytes",
    "usage_used_bytes",
    "usage_free_bytes",
]


def _select_label_id_source(record: Dict[str, Any]) -> Optional[str]:
    for key in ("lsblk_partuuid", "lsblk_ptuuid", "mount_uuid", "volume_id"):
        value = record.get(key)
        if value:
            return str(value)
    return None


def derive_human_id(record: Dict[str, Any]) -> str:
    """Build a short, human-friendly anchor derived from stable identifiers.

    Preference order:
    - lsblk_partuuid
    - lsblk_ptuuid
    - mount_uuid
    - volume_id (composite fallback)
    """

    source = _select_label_id_source(record)
    if not source:
        return ""

    token = str(source).strip()

    # For composite identifiers like "uuid=...|serial=...",
    # focus on the last hex-ish run so we don't print the
    # whole structured string.
    if "=" in token or "|" in token:
        hex_chunks = re.findall(r"[0-9a-fA-F]+", token)
        if hex_chunks:
            token = hex_chunks[-1]

    # Prefer suffix segments after dashes, pulling in extra
    # segments when the last one is very short.
    if "-" in token:
        parts = [p for p in token.split("-") if p]
        if parts:
            acc = parts[-1]
            idx = len(parts) - 2
            while len(acc) < 6 and idx >= 0:
                acc = f"{parts[idx]}-{acc}"
                idx -= 1
            token = acc

    # Clamp to a reasonable label length while keeping the
    # tail stable across exports.
    max_len = 12
    if len(token) > max_len:
        token = token[-max_len:]

    return token


def build_label_rows(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return label-friendly rows for each volume record."""

    rows: List[Dict[str, Any]] = []
    for idx, record in enumerate(records, start=1):
        stable_index = record.get("label_index") or idx
        row: Dict[str, Any] = {
            "label_index": stable_index,
            "human_id": derive_human_id(record),
        }
        for column in LABEL_EXPORT_COLUMNS:
            row[column] = record.get(column)
        rows.append(row)
    return rows


__all__ = ["LABEL_EXPORT_COLUMNS", "derive_human_id", "build_label_rows"]
