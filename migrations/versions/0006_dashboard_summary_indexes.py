"""Add indexes to speed dashboard summary queries."""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0006_dashboard_summary_indexes"
down_revision = "0005_volume_label_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE INDEX IF NOT EXISTS idx_events_volume_path ON events (volume_id, path)")
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_files_last_event_timestamp ON files (last_event_timestamp)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_volumes_last_event_timestamp ON volumes (last_event_timestamp)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_volumes_last_event_timestamp")
    op.execute("DROP INDEX IF EXISTS idx_files_last_event_timestamp")
    op.execute("DROP INDEX IF EXISTS idx_events_volume_path")
