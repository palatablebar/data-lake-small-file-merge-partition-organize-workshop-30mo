"""Celery task: partition-staged compaction with an atomic snapshot publish.

Flow (see README §分区级并发调度与原子发布):
1. Atomically claim the job (``queued+admitted -> running``). A duplicate
   delivery loses the claim and returns without touching anything, so a
   re-delivered task only ever takes effect once.
2. Stage every partition independently: compute the post-merge manifest in
   ``compact_job_partitions.staged_files`` and validate Σrows conservation
   per partition. Live ``data_files`` rows are untouched in this phase.
3. Publish once: inside a single transaction (serialized by a row lock on
   ``lake_tables``) replace the staged partitions, recompute health, append
   the snapshot version and mark the job succeeded. A failure anywhere rolls
   the whole publish back — the table never shows a mix of new and old files.
"""
import time
from datetime import datetime

from app.config.celery_app import celery_app
from app.config.database import SessionLocal
from app.constants.plan_status import PlanStatus
from app.models import CompactJob, CompactJobPartition, CompactPlan, DataFile, LakeTable, PartitionLease
from app.services import job_service, partition_service, schedule_service, snapshot_service
from app.utils.lake import checksum_of
from app.utils.logger import log_event

PROGRESS_PARTITION_BUDGET = 88  # 0-88% per-partition staging, 95% publish, 100% done
SLEEP_PER_GROUP = 0.12          # keeps per-group progress observable in the UI
SLEEP_PER_PARTITION = 0.35


class PartitionValidationError(Exception):
    """A partition's staged rewrite failed row-count / path validation."""


def _claim_job(db, job_id: int) -> bool:
    """Atomic queued+admitted -> running transition; False means duplicate."""
    claimed = (
        db.query(CompactJob)
        .filter(
            CompactJob.id == job_id,
            CompactJob.status == "queued",
            CompactJob.admission_state == "admitted",
        )
        .update(
            {"status": "running", "started_at": datetime.now(), "progress": 1},
            synchronize_session=False,
        )
    )
    db.commit()
    return bool(claimed)


def _groups_by_partition(plan: CompactPlan) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {value: [] for value in (plan.partition_values or [])}
    for group in plan.groups or []:
        grouped.setdefault(group.get("partition_value"), []).append(group)
    return grouped


def _stage_partition(db, table: LakeTable, part: CompactJobPartition, groups: list[dict]) -> dict:
    """Compute the partition's post-merge manifest and validate row counts.

    Raises PartitionValidationError before anything is persisted as live data.
    """
    rows = (
        db.query(DataFile)
        .filter(DataFile.table_id == table.id, DataFile.partition_value == part.partition_value)
        .all()
    )
    files = {row.path: row for row in rows}
    staged = {
        row.path: {
            "path": row.path,
            "size_bytes": int(row.size_bytes),
            "row_count": int(row.row_count),
            "checksum": row.checksum,
            "file_format": row.file_format,
            "write_source": row.write_source,
        }
        for row in rows
    }
    rewritten_files = 0
    produced_files = 0
    bytes_written = 0
    processed_groups = 0

    for group in groups:
        if group.get("action") != "rewrite":
            processed_groups += 1
            continue
        paths = [item["path"] for item in group.get("files", [])]
        missing = [path for path in paths if path not in files]
        if missing:
            raise PartitionValidationError(
                f"分区 {part.partition_value} 缺少输入文件 {missing[0]}（可能被其他任务改写）"
            )
        inputs = [files[path] for path in paths]
        input_rows = sum(int(row.row_count) for row in inputs)
        output_rows = int(group.get("output_rows") or input_rows)
        output_size = int(group.get("output_size_bytes") or sum(int(row.size_bytes) for row in inputs))
        if output_rows != input_rows:
            raise PartitionValidationError(
                f"分区 {part.partition_value} 组 {group.get('group_id')} 行数不守恒："
                f"输入 {input_rows} 行，产物 {output_rows} 行"
            )
        output_path = group["output_file"]
        if output_path in staged and output_path not in paths:
            raise PartitionValidationError(f"分区 {part.partition_value} 产物路径冲突：{output_path}")
        for path in paths:
            staged.pop(path, None)
        staged[output_path] = {
            "path": output_path,
            "size_bytes": output_size,
            "row_count": output_rows,
            "checksum": checksum_of(output_path, output_rows, output_size),
            "file_format": table.file_format,
            "write_source": "compaction",
        }
        rewritten_files += len(paths)
        produced_files += 1
        bytes_written += output_size
        processed_groups += 1
        time.sleep(SLEEP_PER_GROUP)

    rows_before = sum(int(row.row_count) for row in rows)
    rows_after = sum(item["row_count"] for item in staged.values())
    if rows_before != rows_after:
        raise PartitionValidationError(
            f"分区 {part.partition_value} 行数不守恒：{rows_before} → {rows_after}"
        )

    part.group_count = len(groups)
    part.processed_groups = processed_groups
    part.rewritten_files = rewritten_files
    part.produced_files = produced_files
    part.bytes_written = bytes_written
    part.rows_before = rows_before
    part.rows_after = rows_after
    part.row_conserved = True
    part.staged_files = [dict(item, partition_value=part.partition_value) for item in staged.values()]
    part.status = "validated"
    part.finished_at = datetime.now()
    return {
        "rewritten_files": rewritten_files,
        "produced_files": produced_files,
        "bytes_written": bytes_written,
        "rows_before": rows_before,
        "rows_after": rows_after,
    }


def _publish(db, meta: dict, staged_payload: list[dict]) -> object:
    """Apply every staged partition + append the snapshot in ONE transaction.

    The first statement is a row lock on ``lake_tables`` so concurrent jobs
    publish strictly one after another and snapshot versions never collide.
    """
    db.rollback()  # drop any implicit transaction; the lock must come first
    lock_query = db.query(LakeTable).filter(LakeTable.id == meta["table_id"])
    if db.get_bind().dialect.name != "sqlite":  # MySQL: serialize publishes per table
        lock_query = lock_query.with_for_update()
    table = lock_query.one()

    partition_values = [item["partition_value"] for item in staged_payload]
    if partition_values:
        db.query(DataFile).filter(
            DataFile.table_id == table.id, DataFile.partition_value.in_(partition_values)
        ).delete(synchronize_session=False)
    for item in staged_payload:
        for file_entry in item["files"]:
            db.add(
                DataFile(
                    table_id=table.id,
                    partition_value=item["partition_value"],
                    path=file_entry["path"],
                    size_bytes=int(file_entry["size_bytes"]),
                    row_count=int(file_entry["row_count"]),
                    checksum=file_entry["checksum"],
                    file_format=file_entry.get("file_format") or table.file_format,
                    write_source=file_entry.get("write_source") or "compaction",
                )
            )
    db.flush()

    specs = partition_service.recompute_table(db, table, commit=False)
    touched = set(partition_values)
    for spec in specs:
        if spec.partition_value in touched:
            spec.last_compacted_at = datetime.now()

    snapshot = snapshot_service.create_snapshot(
        db,
        table,
        created_by=meta["triggered_by"],
        note=f"合并任务 #{meta['job_id']} 完成快照（{len(touched)} 个分区）",
        job_id=meta["job_id"],
        commit=False,
    )

    job = db.get(CompactJob, meta["job_id"])
    plan = db.get(CompactPlan, meta["plan_id"])
    job.status = "succeeded"
    job.progress = 100
    job.snapshot_id = snapshot.id
    job.snapshot_version = snapshot.version
    job.finished_at = datetime.now()
    if plan is not None:
        plan.status = PlanStatus.COMPLETED.value
    part_rows = (
        db.query(CompactJobPartition).filter(CompactJobPartition.job_id == meta["job_id"]).all()
    )
    for part in part_rows:
        part.status = "published"
        part.snapshot_version = snapshot.version
    db.query(PartitionLease).filter(PartitionLease.job_id == meta["job_id"]).delete(
        synchronize_session=False
    )
    db.commit()
    return snapshot


def _fail(db, job_id: int, reason: str, code: str | None = None) -> None:
    db.rollback()
    try:
        job = db.get(CompactJob, job_id)
        if job is None:
            return
        dangling = (
            db.query(CompactJobPartition)
            .filter(
                CompactJobPartition.job_id == job_id,
                CompactJobPartition.status.in_(("pending", "rewriting")),
            )
            .all()
        )
        for part in dangling:
            part.status = "failed"
            part.error = reason[:900]
            part.finished_at = datetime.now()
        job_service.mark_failed(db, job, f"{code} {reason}" if code else reason)
        plan = db.get(CompactPlan, job.plan_id)
        if plan is not None:
            plan.status = PlanStatus.FAILED.value
            db.commit()
        log_event("JOB_FAILED", job_id=job_id, reason=reason)
    except Exception:
        db.rollback()


@celery_app.task(name="lakepack.run_compaction_job", bind=True)
def run_compaction_job(self, job_id: int) -> dict:  # noqa: ARG001
    db = SessionLocal()
    owns_job = False
    table_id = None
    try:
        job = db.get(CompactJob, job_id)
        if job is None:
            return {"ok": False, "reason": f"job {job_id} not found"}
        table_id = job.table_id
        if not _claim_job(db, job_id):
            db.rollback()
            job = db.get(CompactJob, job_id)
            if job is not None and job.status in ("running", "succeeded"):
                log_event("JOB_DUPLICATE_DELIVERY", job_id=job_id, status=job.status)
                return {"ok": True, "duplicate": True, "job_id": job_id, "status": job.status}
            return {"ok": False, "job_id": job_id, "reason": "job not admitted"}
        owns_job = True

        job = db.get(CompactJob, job_id)
        plan = db.get(CompactPlan, job.plan_id)
        table = db.get(LakeTable, job.table_id)
        log_event("JOB_STARTED", job_id=job.id, plan_id=plan.id)

        grouped = _groups_by_partition(plan)
        partitions = sorted(grouped.keys())
        job.groups_total = sum(len(groups) for groups in grouped.values())
        existing = {
            row.partition_value
            for row in db.query(CompactJobPartition)
            .filter(CompactJobPartition.job_id == job.id)
            .all()
        }
        for value in partitions:
            if value not in existing:
                db.add(
                    CompactJobPartition(
                        job_id=job.id, table_id=table.id, partition_value=value, status="pending"
                    )
                )
        db.commit()

        totals = {
            "processed_groups": 0,
            "rewritten_files": 0,
            "produced_files": 0,
            "bytes_written": 0,
            "rows_before": 0,
            "rows_after": 0,
        }
        for index, value in enumerate(partitions, start=1):
            part = (
                db.query(CompactJobPartition)
                .filter(
                    CompactJobPartition.job_id == job.id,
                    CompactJobPartition.partition_value == value,
                )
                .one()
            )
            part.status = "rewriting"
            part.started_at = datetime.now()
            db.commit()

            metrics = _stage_partition(db, table, part, grouped[value])
            totals["rewritten_files"] += metrics["rewritten_files"]
            totals["produced_files"] += metrics["produced_files"]
            totals["bytes_written"] += metrics["bytes_written"]
            totals["rows_before"] += metrics["rows_before"]
            totals["rows_after"] += metrics["rows_after"]
            totals["processed_groups"] += part.processed_groups

            job_service.update_progress(
                db,
                job,
                progress=int(index / max(1, len(partitions)) * PROGRESS_PARTITION_BUDGET),
                processed_groups=totals["processed_groups"],
                rewritten_files=totals["rewritten_files"],
                produced_files=totals["produced_files"],
                bytes_written=totals["bytes_written"],
                rows_before=totals["rows_before"],
                rows_after=totals["rows_after"],
            )
            time.sleep(SLEEP_PER_PARTITION)

        job.progress = 95
        db.commit()

        part_rows = (
            db.query(CompactJobPartition).filter(CompactJobPartition.job_id == job.id).all()
        )
        staged_payload = [
            {
                "partition_value": row.partition_value,
                "files": [dict(entry) for entry in (row.staged_files or [])],
            }
            for row in part_rows
        ]
        meta = {
            "job_id": job.id,
            "plan_id": plan.id,
            "table_id": table.id,
            "triggered_by": job.triggered_by,
        }
        snapshot = _publish(db, meta, staged_payload)
        log_event(
            "JOB_SUCCEEDED",
            job_id=job.id,
            plan_id=plan.id,
            rewritten_files=totals["rewritten_files"],
            produced_files=totals["produced_files"],
            snapshot_version=snapshot.version,
        )
        return {
            "ok": True,
            "job_id": job.id,
            "rewritten_files": totals["rewritten_files"],
            "produced_files": totals["produced_files"],
            "snapshot_id": snapshot.id,
            "snapshot_version": snapshot.version,
        }
    except PartitionValidationError as exc:
        _fail(db, job_id, str(exc), code="LAKE-2020")
        return {"ok": False, "job_id": job_id, "reason": str(exc)}
    except Exception as exc:  # pragma: no cover - defensive worker path
        _fail(db, job_id, f"{type(exc).__name__}: {exc}")
        return {"ok": False, "job_id": job_id, "reason": str(exc)}
    finally:
        if owns_job and table_id is not None:
            try:
                schedule_service.release_job_leases(db, table_id, job_id)
                schedule_service.schedule_table(db, table_id)
            except Exception:
                db.rollback()
        db.close()
