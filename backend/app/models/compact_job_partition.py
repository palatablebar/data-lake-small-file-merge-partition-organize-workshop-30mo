from datetime import datetime

from sqlalchemy import JSON, BigInteger, Boolean, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class CompactJobPartition(Base):
    """Per-partition staged rewrite record for one compact job.

    The worker stages each partition independently (``staged_files`` is the
    partition's post-merge manifest) and validates row conservation before the
    job publishes anything. Live ``data_files`` rows are only replaced during
    the single atomic publish transaction, so a failed partition never leaves
    the table half-rewritten.
    """

    __tablename__ = "compact_job_partitions"
    __table_args__ = (UniqueConstraint("job_id", "partition_value", name="uq_job_partition"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(Integer, ForeignKey("compact_jobs.id", ondelete="CASCADE"), index=True)
    table_id: Mapped[int] = mapped_column(Integer, ForeignKey("lake_tables.id", ondelete="CASCADE"), index=True)
    partition_value: Mapped[str] = mapped_column(String(32), nullable=False)
    # status: pending | rewriting | validated | published | failed
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    group_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    processed_groups: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rewritten_files: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    produced_files: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    bytes_written: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    rows_before: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    rows_after: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    row_conserved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    staged_files: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    snapshot_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    def __repr__(self) -> str:  # pragma: no cover
        return f"<CompactJobPartition job={self.job_id} {self.partition_value} {self.status}>"
