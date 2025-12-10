"""Add jobs table for tracking active tasks."""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0004_jobs_table"
down_revision = "0003_volume_mount_metadata"
branch_labels = None
depends_on = None


_DEF_STATUS_INDEX = "idx_jobs_status"
_DEF_VOLUME_INDEX = "idx_jobs_volume"


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())
    if "jobs" in existing_tables:
        # Catalogs created from the static schema already include the jobs
        # table and its indexes; skip creation in that case.
        return

    op.create_table(
        "jobs",
        sa.Column("job_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("job_type", sa.Text(), nullable=False),
        sa.Column("path", sa.Text(), nullable=True),
        sa.Column("volume_id", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, default="running"),
        sa.Column("progress_json", sa.Text(), nullable=True),
        sa.Column("owner_pid", sa.Text(), nullable=True),
        sa.Column("owner_host", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.Text(), nullable=False),
        sa.Column("completed_at", sa.Text(), nullable=True),
    )
    op.create_index(_DEF_STATUS_INDEX, "jobs", ["status"])
    op.create_index(_DEF_VOLUME_INDEX, "jobs", ["volume_id"])


def downgrade() -> None:
    op.drop_index(_DEF_VOLUME_INDEX, table_name="jobs")
    op.drop_index(_DEF_STATUS_INDEX, table_name="jobs")
    op.drop_table("jobs")
