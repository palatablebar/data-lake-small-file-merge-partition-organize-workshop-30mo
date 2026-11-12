"""Partition-level admission scheduler (replaces the old table-wide lock).

Semantics (see README §分区级并发调度):
- A job is admitted only when none of its partitions overlaps or is adjacent
  to a partition leased by another admitted/running job of the same table.
- Leases live in ``partition_leases`` (unique per table+partition) and are
  released when the owning job reaches a terminal state.
- Admission is serialized per table with a row lock on ``lake_tables`` so two
  schedulers (API + worker) can never grant conflicting leases concurrently.
- Admitted jobs are dispatched to Celery; a duplicate delivery is neutralised
  by the worker's atomic claim, so re-dispatching is always safe.
"""
from datetime import datetime

from sqlalchemy.orm import Session

from app.models import CompactJob, CompactPlan, LakeTable, PartitionLease, PartitionSpec
from app.schemas.common import iso
from app.services import snapshot_service
from app.services.partition_conflict import describe_conflicts, ordered_universe
from app.utils.logger import log_event

ACTIVE_JOB_STATUSES = ("queued", "running")


def _lock_table(db: Session, table_id: int) -> LakeTable | None:
    query = db.query(LakeTable).filter(LakeTable.id == table_id)
    if db.get_bind().dialect.name != "sqlite":  # MySQL: serialize admission per table
        query = query.with_for_update()
    return query.first()


def _cleanup_stale_leases(db: Session, table_id: int) -> None:
    """Drop leases whose job is terminal or was never admitted (crash leftovers)."""
    leases = db.query(PartitionLease).filter(PartitionLease.table_id == table_id).all()
    for lease in leases:
        job = db.get(CompactJob, lease.job_id)
        stale = (
            job is None
            or job.status not in ACTIVE_JOB_STATUSES
            or (job.status == "queued" and job.admission_state != "admitted")
        )
        if stale:
            db.delete(lease)
    db.flush()


def _partition_universe(db: Session, table_id: int, extra_values: list[str]) -> list[str]:
    spec_values = [
        row[0]
        for row in db.query(PartitionSpec.partition_value).filter(PartitionSpec.table_id == table_id).all()
    ]
    lease_values = [
        row[0]
        for row in db.query(PartitionLease.partition_value).filter(PartitionLease.table_id == table_id).all()
    ]
    return ordered_universe(spec_values, lease_values, extra_values)


def _occupied_map(db: Session, table_id: int) -> dict[str, int]:
    leases = db.query(PartitionLease).filter(PartitionLease.table_id == table_id).all()
    return {lease.partition_value: lease.job_id for lease in leases}


def _job_partitions(db: Session, job: CompactJob) -> list[str]:
    plan = db.get(CompactPlan, job.plan_id)
    if plan is None:
        return []
    return sorted({str(value) for value in (plan.partition_values or [])})


def _waiting_reason(conflict_result: dict) -> str:
    first = conflict_result["conflicts"][0]
    if first["reason"] == "overlap":
        base = f"分区 {first['requested']} 与任务 #{first['job_id']} 重叠"
    else:
        base = f"分区 {first['requested']} 与任务 #{first['job_id']} 的 {first['conflicting']} 相邻"
    extra = len(conflict_result["conflicts"]) - 1
    if extra > 0:
        base = f"{base}（另有 {extra} 处冲突）"
    return f"{base}，等待释放"


def _safe_dispatch(db: Session, job: CompactJob) -> None:
    """Send the Celery task; on broker failure roll the lease back to waiting."""
    try:
        from app.tasks.compact_job import run_compaction_job

        async_result = run_compaction_job.delay(job.id)
        job.celery_task_id = async_result.id
        db.commit()
        log_event("JOB_DISPATCHED", job_id=job.id, task_id=job.celery_task_id)
    except Exception as exc:  # broker down -> keep the job queued, not stuck
        db.rollback()
        db.query(PartitionLease).filter(PartitionLease.job_id == job.id).delete(synchronize_session=False)
        job.admission_state = "waiting"
        job.admitted_at = None
        job.waiting_reason = f"任务投递失败（{type(exc).__name__}），等待重试调度"
        db.commit()
        log_event("JOB_DISPATCH_FAILED", job_id=job.id, reason=str(exc))


def schedule_table(db: Session, table_id: int) -> None:
    """Admit every queued job whose partitions are free, then dispatch tasks."""
    table = _lock_table(db, table_id)
    if table is None:
        return
    _cleanup_stale_leases(db, table_id)

    waiting_jobs = (
        db.query(CompactJob)
        .filter(
            CompactJob.table_id == table_id,
            CompactJob.status == "queued",
            CompactJob.admission_state == "waiting",
        )
        .order_by(CompactJob.id.asc())
        .all()
    )
    extra_values: list[str] = []
    for job in waiting_jobs:
        extra_values.extend(_job_partitions(db, job))
    universe = _partition_universe(db, table_id, extra_values)
    occupied = _occupied_map(db, table_id)

    for job in waiting_jobs:
        partitions = _job_partitions(db, job)
        if not partitions:
            job.waiting_reason = "计划没有可调度分区"
            continue
        result = describe_conflicts(partitions, occupied, universe)
        if result["conflicts"]:
            job.waiting_reason = _waiting_reason(result)
            log_event(
                "JOB_ADMISSION_BLOCKED",
                job_id=job.id,
                blocked_by=result["job_ids"],
                reason=job.waiting_reason,
            )
            continue
        now = datetime.now()
        for value in partitions:
            db.add(PartitionLease(table_id=table_id, partition_value=value, job_id=job.id))
            occupied[value] = job.id
        job.admission_state = "admitted"
        job.admitted_at = now
        job.waiting_reason = None
        log_event("JOB_ADMITTED", job_id=job.id, partitions=partitions)
    db.commit()

    # Dispatch admitted jobs that have no broker task yet (new ones plus any
    # job that crashed between admission and dispatch). Re-dispatch is safe:
    # the worker claims the job atomically and ignores duplicate deliveries.
    pending_dispatch = (
        db.query(CompactJob)
        .filter(
            CompactJob.table_id == table_id,
            CompactJob.status == "queued",
            CompactJob.admission_state == "admitted",
            CompactJob.celery_task_id.is_(None),
        )
        .order_by(CompactJob.id.asc())
        .all()
    )
    for job in pending_dispatch:
        _safe_dispatch(db, job)


def release_job_leases(db: Session, table_id: int, job_id: int) -> None:
    db.query(PartitionLease).filter(
        PartitionLease.table_id == table_id, PartitionLease.job_id == job_id
    ).delete(synchronize_session=False)
    db.commit()


def schedule_overview(db: Session, table_id: int | None = None) -> dict:
    """Read model for the UI: occupied partitions, waiting queue, snapshots."""
    query = db.query(LakeTable).order_by(LakeTable.db_name, LakeTable.table_name)
    if table_id is not None:
        query = query.filter(LakeTable.id == table_id)
    tables = query.all()

    items: list[dict] = []
    occupied_total = 0
    waiting_total = 0
    for table in tables:
        leases = (
            db.query(PartitionLease)
            .filter(PartitionLease.table_id == table.id)
            .order_by(PartitionLease.partition_value.asc())
            .all()
        )
        waiting_jobs = (
            db.query(CompactJob)
            .filter(
                CompactJob.table_id == table.id,
                CompactJob.status == "queued",
                CompactJob.admission_state == "waiting",
            )
            .order_by(CompactJob.id.asc())
            .all()
        )
        active_ids = sorted({lease.job_id for lease in leases})
        active_jobs = (
            db.query(CompactJob).filter(CompactJob.id.in_(active_ids)).order_by(CompactJob.id.asc()).all()
            if active_ids
            else []
        )
        status_by_id = {job.id: job.status for job in active_jobs}
        occupied = [
            {
                "partition_value": lease.partition_value,
                "job_id": lease.job_id,
                "job_status": status_by_id.get(lease.job_id, "unknown"),
            }
            for lease in leases
        ]
        occupied_map = {lease.partition_value: lease.job_id for lease in leases}
        extra_values: list[str] = []
        for job in waiting_jobs:
            extra_values.extend(_job_partitions(db, job))
        universe = _partition_universe(db, table.id, extra_values)

        waiting = []
        for position, job in enumerate(waiting_jobs, start=1):
            partitions = _job_partitions(db, job)
            conflicts = describe_conflicts(partitions, occupied_map, universe)
            waiting.append(
                {
                    "job_id": job.id,
                    "partitions": partitions,
                    "position": position,
                    "reason": job.waiting_reason,
                    "blocked_by": conflicts["job_ids"],
                    "created_at": iso(job.created_at),
                }
            )
        latest = snapshot_service.latest_snapshot(db, table.id)
        items.append(
            {
                "table_id": table.id,
                "table_fq_name": table.fq_name,
                "partition_key": table.partition_key,
                "occupied": occupied,
                "active_jobs": [
                    {
                        "job_id": job.id,
                        "status": job.status,
                        "progress": int(job.progress or 0),
                        "partitions": _job_partitions(db, job),
                    }
                    for job in active_jobs
                ],
                "waiting": waiting,
                "latest_snapshot": (
                    {
                        "id": latest.id,
                        "version": latest.version,
                        "job_id": latest.job_id,
                        "created_at": iso(latest.created_at),
                    }
                    if latest is not None
                    else None
                ),
            }
        )
        occupied_total += len(occupied)
        waiting_total += len(waiting)
    return {
        "tables": items,
        "occupied_partition_count": occupied_total,
        "waiting_count": waiting_total,
    }
