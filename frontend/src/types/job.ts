export type AdmissionState = 'waiting' | 'admitted'

export type PartitionTaskStatus = 'pending' | 'rewriting' | 'validated' | 'published' | 'failed'

export interface JobPartitionTask {
  partition_value: string
  status: PartitionTaskStatus
  group_count: number
  processed_groups: number
  rewritten_files: number
  produced_files: number
  bytes_written: number
  rows_before: number
  rows_after: number
  row_conserved: boolean
  error: string | null
  snapshot_version: number | null
  started_at: string | null
  finished_at: string | null
}

export interface CompactJob {
  id: number
  plan_id: number
  table_id: number
  table_fq_name: string | null
  partition_key: string
  partition_values: string[]
  strategy: string | null
  fingerprint: string | null
  status: 'queued' | 'running' | 'succeeded' | 'failed'
  progress: number
  groups_total: number
  processed_groups: number
  rewritten_files: number
  produced_files: number
  bytes_written: number
  rows_before: number
  rows_after: number
  row_conserved: boolean
  snapshot_id: number | null
  snapshot_version: number | null
  celery_task_id: string | null
  failure_reason: string | null
  admission_state: AdmissionState
  waiting_reason: string | null
  admitted_at: string | null
  queue_position: number | null
  partitions: JobPartitionTask[]
  triggered_by: number | null
  triggered_by_name: string | null
  started_at: string | null
  finished_at: string | null
  created_at: string | null
  est_duration_seconds: number | null
  duplicate?: boolean
}

export interface ScheduleOccupied {
  partition_value: string
  job_id: number
  job_status: string
}

export interface ScheduleActiveJob {
  job_id: number
  status: string
  progress: number
  partitions: string[]
}

export interface ScheduleWaitingJob {
  job_id: number
  partitions: string[]
  position: number
  reason: string | null
  blocked_by: number[]
  created_at: string | null
}

export interface ScheduleSnapshot {
  id: number
  version: number
  job_id: number | null
  created_at: string | null
}

export interface ScheduleTable {
  table_id: number
  table_fq_name: string
  partition_key: string
  occupied: ScheduleOccupied[]
  active_jobs: ScheduleActiveJob[]
  waiting: ScheduleWaitingJob[]
  latest_snapshot: ScheduleSnapshot | null
}

export interface JobSchedule {
  tables: ScheduleTable[]
  occupied_partition_count: number
  waiting_count: number
}

export interface JobListResponse {
  items: CompactJob[]
  schedule: JobSchedule
}
