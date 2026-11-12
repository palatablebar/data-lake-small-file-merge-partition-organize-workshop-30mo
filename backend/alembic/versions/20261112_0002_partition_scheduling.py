"""partition-level scheduling + staged atomic publish

- compact_jobs: admission_state / waiting_reason / admitted_at /
  snapshot_version / idempotency_key columns
- new tables: partition_leases, compact_job_partitions
"""
import sqlalchemy as sa
from alembic import op

revision = "20261112_0002"
down_revision = "20261110_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "compact_jobs",
        sa.Column("admission_state", sa.String(16), nullable=False, server_default="waiting"),
    )
    op.add_column("compact_jobs", sa.Column("waiting_reason", sa.Text(), nullable=True))
    op.add_column("compact_jobs", sa.Column("admitted_at", sa.DateTime(), nullable=True))
    op.add_column("compact_jobs", sa.Column("snapshot_version", sa.Integer(), nullable=True))
    op.add_column("compact_jobs", sa.Column("idempotency_key", sa.String(64), nullable=True))
    op.create_unique_constraint("uq_compact_job_idempotency", "compact_jobs", ["idempotency_key"])

    op.create_table(
        "partition_leases",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("table_id", sa.Integer(), sa.ForeignKey("lake_tables.id", ondelete="CASCADE"), nullable=False),
        sa.Column("partition_value", sa.String(32), nullable=False),
        sa.Column("job_id", sa.Integer(), sa.ForeignKey("compact_jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("acquired_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("table_id", "partition_value", name="uq_partition_lease"),
    )
    op.create_index("ix_partition_leases_table_id", "partition_leases", ["table_id"])
    op.create_index("ix_partition_leases_job_id", "partition_leases", ["job_id"])

    op.create_table(
        "compact_job_partitions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("job_id", sa.Integer(), sa.ForeignKey("compact_jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("table_id", sa.Integer(), sa.ForeignKey("lake_tables.id", ondelete="CASCADE"), nullable=False),
        sa.Column("partition_value", sa.String(32), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("group_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("processed_groups", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rewritten_files", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("produced_files", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("bytes_written", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("rows_before", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("rows_after", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("row_conserved", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("staged_files", sa.JSON(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("snapshot_version", sa.Integer(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("job_id", "partition_value", name="uq_job_partition"),
    )
    op.create_index("ix_compact_job_partitions_job_id", "compact_job_partitions", ["job_id"])
    op.create_index("ix_compact_job_partitions_table_id", "compact_job_partitions", ["table_id"])


def downgrade() -> None:
    op.drop_table("compact_job_partitions")
    op.drop_table("partition_leases")
    op.drop_constraint("uq_compact_job_idempotency", "compact_jobs", type_="unique")
    for column in ("idempotency_key", "snapshot_version", "admitted_at", "waiting_reason", "admission_state"):
        op.drop_column("compact_jobs", column)
