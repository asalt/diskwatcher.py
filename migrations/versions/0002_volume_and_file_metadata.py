"""Add volumes and files metadata tables."""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0002_volume_and_file_metadata"
down_revision = "0001_initial_catalog"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS volumes (
            volume_id TEXT PRIMARY KEY,
            directory TEXT NOT NULL,
            event_count INTEGER NOT NULL DEFAULT 0,
            created_count INTEGER NOT NULL DEFAULT 0,
            modified_count INTEGER NOT NULL DEFAULT 0,
            deleted_count INTEGER NOT NULL DEFAULT 0,
            last_event_timestamp TEXT,
            usage_total_bytes INTEGER,
            usage_used_bytes INTEGER,
            usage_free_bytes INTEGER,
            usage_refreshed_at TEXT,
            events_since_refresh INTEGER NOT NULL DEFAULT 0
        );
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS files (
            volume_id TEXT NOT NULL,
            path TEXT NOT NULL,
            directory TEXT NOT NULL,
            size_bytes INTEGER,
            modified_time TEXT,
            created_time TEXT,
            last_event_timestamp TEXT,
            last_event_type TEXT,
            is_deleted INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (volume_id, path)
        );
        """
    )

    op.execute(
        """CREATE INDEX IF NOT EXISTS idx_files_directory ON files (directory);"""
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_files_directory;")
    op.execute("DROP TABLE IF EXISTS files;")
    op.execute("DROP TABLE IF EXISTS volumes;")
