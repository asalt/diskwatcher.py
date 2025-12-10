"""Alembic helpers for managing catalog schema migrations."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from alembic import command
from alembic.config import Config

from diskwatcher.db.connection import DB_PATH

_DEFAULT_ALEMBIC_INI = Path(__file__).resolve().parent.parent.parent / "alembic.ini"
BASELINE_REVISION = "0002_volume_and_file_metadata"


def build_alembic_config(
    *,
    ini_path: Optional[Path] = None,
    database_url: Optional[str] = None,
) -> Config:
    """Return an Alembic ``Config`` primed for the current workspace."""
    ini = ini_path or _DEFAULT_ALEMBIC_INI
    if not ini.exists():
        # Fallback for editable installs or sandbox runs where the default
        # path resolves to the package but the Alembic INI lives alongside
        # the repository root.
        candidate = Path.cwd() / "alembic.ini"
        if candidate.exists():
            ini = candidate

    config = Config(str(ini))
    if database_url is None:
        database_url = f"sqlite:///{DB_PATH}"
    config.set_main_option("sqlalchemy.url", database_url)

    # Ensure script_location is present even if the INI was minimal or missing.
    script_location = config.get_main_option("script_location")
    if not script_location:
        # Default to a "migrations" directory next to the INI file.
        config.set_main_option(
            "script_location",
            str(Path(ini).parent / "migrations"),
        )
    return config


def upgrade(
    *,
    revision: str = "head",
    ini_path: Optional[Path] = None,
    database_url: Optional[str] = None,
) -> None:
    """Upgrade the catalog schema to the requested revision."""

    config = build_alembic_config(ini_path=ini_path, database_url=database_url)
    command.upgrade(config, revision)


def stamp(
    *,
    revision: str,
    ini_path: Optional[Path] = None,
    database_url: Optional[str] = None,
) -> None:
    """Stamp the database with a specific revision without running migrations."""

    config = build_alembic_config(ini_path=ini_path, database_url=database_url)
    command.stamp(config, revision)
