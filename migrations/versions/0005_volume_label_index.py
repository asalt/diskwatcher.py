"""Add label_index column to volumes for stable label IDs."""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0005_volume_label_index"
down_revision = "0004_jobs_table"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("volumes", sa.Column("label_index", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("volumes", "label_index")

