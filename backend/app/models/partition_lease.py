from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class PartitionLease(Base):
    """Admission lease for one partition of one table.

    A row exists only while the owning job is admitted/running; the unique
    (table_id, partition_value) constraint makes two admitted jobs physically
    unable to hold the same partition. Adjacency conflicts are evaluated by
    the scheduler before the lease rows are inserted.
    """

    __tablename__ = "partition_leases"
    __table_args__ = (UniqueConstraint("table_id", "partition_value", name="uq_partition_lease"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    table_id: Mapped[int] = mapped_column(Integer, ForeignKey("lake_tables.id", ondelete="CASCADE"), index=True)
    partition_value: Mapped[str] = mapped_column(String(32), nullable=False)
    job_id: Mapped[int] = mapped_column(Integer, ForeignKey("compact_jobs.id", ondelete="CASCADE"), index=True)
    acquired_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    def __repr__(self) -> str:  # pragma: no cover
        return f"<PartitionLease table={self.table_id} {self.partition_value} job={self.job_id}>"
