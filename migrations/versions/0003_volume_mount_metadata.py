"""Store mount identity metadata on volumes."""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0003_volume_mount_metadata"
down_revision = "0002_volume_and_file_metadata"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Some catalogs may have been created from the static schema before this
    # migration was introduced, in which case the mount_* and lsblk_* columns
    # already exist. Skip the ALTER TABLE statements when we detect that shape.
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    existing_columns = {col["name"] for col in inspector.get_columns("volumes")}
    if "mount_device" in existing_columns:
        return

    op.add_column("volumes", sa.Column("mount_device", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("mount_point", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("mount_uuid", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("mount_label", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("mount_volume_id", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("lsblk_name", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("lsblk_path", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("lsblk_model", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("lsblk_serial", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("lsblk_vendor", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("lsblk_size", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("lsblk_fsver", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("lsblk_pttype", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("lsblk_ptuuid", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("lsblk_parttype", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("lsblk_partuuid", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("lsblk_parttypename", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("lsblk_wwn", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("lsblk_maj_min", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("lsblk_json", sa.Text(), nullable=True))
    op.add_column("volumes", sa.Column("identity_refreshed_at", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("volumes", "identity_refreshed_at")
    op.drop_column("volumes", "lsblk_json")
    op.drop_column("volumes", "lsblk_maj_min")
    op.drop_column("volumes", "lsblk_wwn")
    op.drop_column("volumes", "lsblk_parttypename")
    op.drop_column("volumes", "lsblk_partuuid")
    op.drop_column("volumes", "lsblk_parttype")
    op.drop_column("volumes", "lsblk_ptuuid")
    op.drop_column("volumes", "lsblk_pttype")
    op.drop_column("volumes", "lsblk_fsver")
    op.drop_column("volumes", "lsblk_size")
    op.drop_column("volumes", "lsblk_vendor")
    op.drop_column("volumes", "lsblk_serial")
    op.drop_column("volumes", "lsblk_model")
    op.drop_column("volumes", "lsblk_path")
    op.drop_column("volumes", "lsblk_name")
    op.drop_column("volumes", "mount_volume_id")
    op.drop_column("volumes", "mount_label")
    op.drop_column("volumes", "mount_uuid")
    op.drop_column("volumes", "mount_point")
    op.drop_column("volumes", "mount_device")
