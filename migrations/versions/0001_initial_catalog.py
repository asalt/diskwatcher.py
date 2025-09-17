"""Baseline catalog schema."""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0001_initial_catalog"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            event_type TEXT NOT NULL,
            path TEXT NOT NULL,
            directory TEXT NOT NULL,
            volume_id TEXT NOT NULL,
            process_id TEXT
        );
        """
    )
    op.execute(
        """CREATE INDEX IF NOT EXISTS idx_events_path ON events (path);"""
    )
    op.execute(
        """CREATE INDEX IF NOT EXISTS idx_events_volume ON events (volume_id);"""
    )
    op.execute(
        """CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp);"""
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_events_timestamp;")
    op.execute("DROP INDEX IF EXISTS idx_events_volume;")
    op.execute("DROP INDEX IF EXISTS idx_events_path;")
    op.execute("DROP TABLE IF EXISTS events;")
