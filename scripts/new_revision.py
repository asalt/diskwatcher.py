"""Helper for creating Alembic revisions within the repo workspace."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from alembic import command

from diskwatcher.db.migration import build_alembic_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--message", "-m", required=True, help="Migration message excerpt")
    parser.add_argument(
        "--autogenerate",
        action="store_true",
        help="Run Alembic autogenerate against connected database",
    )
    parser.add_argument(
        "--url",
        help="Override the database URL (defaults to ~/.diskwatcher/diskwatcher.db)",
    )
    parser.add_argument(
        "--ini",
        type=Path,
        help="Path to an Alembic ini file (defaults to repository alembic.ini)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = build_alembic_config(
        ini_path=args.ini,
        database_url=args.url,
    )
    command.revision(
        config,
        message=args.message,
        autogenerate=args.autogenerate,
    )


if __name__ == "__main__":  # pragma: no cover - exercised indirectly in docs
    main()
