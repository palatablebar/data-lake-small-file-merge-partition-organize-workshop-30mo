import http from './http'
import type { CompactJob, JobListResponse } from '../types/job'

export async function fetchJobs(tableId?: number): Promise<JobListResponse> {
  const { data } = await http.get('/jobs', { params: tableId ? { table_id: tableId } : {} })
  return data
}

export async function fetchJob(jobId: number): Promise<{ job: CompactJob }> {
  const { data } = await http.get(`/jobs/${jobId}`)
  return data
}

export async function retryJob(jobId: number): Promise<{ job: CompactJob }> {
  const { data } = await http.post(`/jobs/${jobId}/retry`)
  return data
}
