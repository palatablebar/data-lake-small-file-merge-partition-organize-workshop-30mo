<template>
  <div>
    <PageHeader
      figure="FIG.04"
      section="execution monitor"
      :title="'任务执行监控'"
      description="按分区租约并发调度：互不重叠且不相邻的分区同时重写；每个分区先独立校验行数，全部通过后统一发布一个快照版本。"
    >
      <template #actions>
        <button class="bp-btn" :disabled="jobStore.loading" @click="refresh">刷新</button>
        <RouterLink to="/snapshots" class="bp-btn">快照校验</RouterLink>
      </template>
    </PageHeader>

    <ErrorNotice :error="jobStore.error" />

    <div class="mt-4 grid gap-4 lg:grid-cols-3" data-test="schedule-board">
      <section class="bp-frame" data-test="schedule-occupied">
        <div class="bp-frame__head">
          <span class="label">分区占用</span>
          <span class="num text-[10.5px]">{{ schedule?.occupied_partition_count ?? 0 }} 分区</span>
        </div>
        <div class="max-h-[190px] overflow-auto">
          <div
            v-for="table in occupiedTables"
            :key="table.table_id"
            class="border-b border-[var(--rule-soft)] px-3 py-2.5 last:border-b-0"
          >
            <div class="flex items-center justify-between">
              <span class="font-mono text-[11px]">{{ table.table_fq_name }}</span>
              <span class="label">{{ table.occupied.length }} 分区</span>
            </div>
            <div class="mt-1.5 flex flex-wrap gap-1.5">
              <span
                v-for="item in table.occupied"
                :key="`${table.table_id}-${item.partition_value}`"
                class="bp-tag bp-tag--watch font-mono !normal-case !tracking-normal"
                :data-test="`occupied-${table.table_id}-${item.partition_value}`"
              >
                {{ table.partition_key }}={{ item.partition_value }} · #{{ item.job_id }}
              </span>
            </div>
          </div>
          <div v-if="!occupiedTables.length" class="px-4 py-6 text-center text-[11px] text-[var(--ink-faint)]">
            当前没有任务占用分区。
          </div>
        </div>
      </section>

      <section class="bp-frame" data-test="schedule-waiting">
        <div class="bp-frame__head">
          <span class="label">等待队列</span>
          <span class="num text-[10.5px]">{{ schedule?.waiting_count ?? 0 }} 任务</span>
        </div>
        <div class="max-h-[190px] overflow-auto">
          <div
            v-for="entry in waitingEntries"
            :key="`${entry.table.table_id}-${entry.job.job_id}`"
            class="border-b border-[var(--rule-soft)] px-3 py-2.5 last:border-b-0"
            :data-test="`waiting-job-${entry.job.job_id}`"
          >
            <div class="flex items-center gap-2">
              <span class="bp-tag bp-tag--muted">#{{ entry.job.position }}</span>
              <span class="font-display text-[12px]">任务 #{{ entry.job.job_id }}</span>
              <span class="ml-auto font-mono text-[10px] text-[var(--ink-soft)]">{{ entry.table.table_fq_name }}</span>
            </div>
            <div class="mt-1 font-mono text-[10.5px] text-[var(--ink-soft)]">
              {{ entry.table.partition_key }}={{ entry.job.partitions.join(', ') }}
            </div>
            <div v-if="entry.job.reason" class="mt-1 text-[10.5px] text-[var(--ochre)]">{{ entry.job.reason }}</div>
          </div>
          <div v-if="!waitingEntries.length" class="px-4 py-6 text-center text-[11px] text-[var(--ink-faint)]">
            等待队列为空，没有分区冲突。
          </div>
        </div>
      </section>

      <section class="bp-frame" data-test="schedule-snapshots">
        <div class="bp-frame__head">
          <span class="label">已发布快照版本</span>
          <span class="num text-[10.5px]">{{ snapshotTables.length }} 表</span>
        </div>
        <div class="max-h-[190px] overflow-auto">
          <div
            v-for="table in snapshotTables"
            :key="table.table_id"
            class="flex items-center gap-2.5 border-b border-[var(--rule-soft)] px-3 py-2.5 last:border-b-0"
            :data-test="`snapshot-version-${table.table_id}`"
          >
            <span class="font-mono text-[11px]">{{ table.table_fq_name }}</span>
            <span class="bp-tag bp-tag--ok ml-auto">v{{ table.latest_snapshot?.version }}</span>
            <span class="text-[10px] text-[var(--ink-faint)]">
              {{ table.latest_snapshot?.job_id ? `任务 #${table.latest_snapshot.job_id}` : '手工' }}
            </span>
          </div>
          <div v-if="!snapshotTables.length" class="px-4 py-6 text-center text-[11px] text-[var(--ink-faint)]">
            还没有发布过快照。
          </div>
        </div>
      </section>
    </div>

    <div class="mt-4 grid gap-5 xl:grid-cols-[360px_minmax(0,1fr)]">
      <section class="bp-frame" data-test="job-list">
        <div class="bp-frame__head">
          <span class="label">任务台账</span>
          <span class="num text-[10.5px]">{{ jobStore.items.length }} 条</span>
        </div>
        <div class="max-h-[560px] overflow-auto">
          <button
            v-for="job in jobStore.items"
            :key="job.id"
            class="block w-full border-b border-[var(--rule-soft)] px-3 py-2.5 text-left transition-colors last:border-b-0 hover:bg-[var(--paper)]"
            :class="selectedId === job.id ? 'bg-[var(--paper)]' : ''"
            :data-test="`job-item-${job.id}`"
            @click="select(job.id)"
          >
            <div class="flex items-center gap-2.5">
              <span class="font-display text-[12px]">#{{ job.id }}</span>
              <StatusTag :status="job.status" kind="job" />
              <span class="ml-auto num text-[10.5px]">{{ job.progress }}%</span>
            </div>
            <div class="mt-1 truncate text-[11px] text-[var(--ink-soft)]">{{ job.table_fq_name }}</div>
            <div class="mt-1 flex items-center justify-between text-[10px] text-[var(--ink-faint)]">
              <span class="font-mono">{{ job.partition_values.join(' / ') }}</span>
              <span>{{ job.created_at }}</span>
            </div>
          </button>
          <div v-if="!jobStore.items.length" class="px-4 py-10 text-center text-[11.5px] text-[var(--ink-faint)]">
            还没有合并任务。先到湖表分区明细生成计划并提交执行。
          </div>
        </div>
      </section>

      <main class="min-w-0 space-y-4" data-test="job-detail">
        <template v-if="selected">
          <JobProgress :job="selected" />
          <PartitionRunTable
            v-if="selected.partitions && selected.partitions.length"
            :partitions="selected.partitions"
            :partition-key="selected.partition_key"
          />
          <div class="grid gap-4 md:grid-cols-2">
            <div class="bp-frame">
              <div class="bp-frame__head"><span class="label">关联计划</span></div>
              <div class="bp-frame__body space-y-1.5 text-[11.5px]">
                <div class="flex items-center justify-between">
                  <span class="text-[var(--ink-soft)]">计划 ID</span><span class="num">#{{ selected.plan_id }}</span>
                </div>
                <div class="flex items-center justify-between">
                  <span class="text-[var(--ink-soft)]">策略</span><span class="font-mono text-[11px]">{{ selected.strategy }}</span>
                </div>
                <div class="flex items-center justify-between">
                  <span class="text-[var(--ink-soft)]">分区</span><span class="font-mono text-[11px]">{{ selected.partition_values.join(', ') }}</span>
                </div>
                <div class="flex items-center justify-between">
                  <span class="text-[var(--ink-soft)]">指纹</span><span class="font-mono text-[10px]">{{ selected.fingerprint }}</span>
                </div>
              </div>
            </div>
            <div class="bp-frame">
              <div class="bp-frame__head"><span class="label">下一步</span></div>
              <div class="bp-frame__body">
                <div v-if="selected.snapshot_version" class="text-[11.5px]">
                  任务已统一发布快照 <span class="num">v{{ selected.snapshot_version }}</span>，
                  可以去快照校验页对比前后 manifest、确认行数守恒。
                </div>
                <div v-else-if="selected.status === 'failed'" class="text-[11.5px]">
                  任务失败，未发布任何分区变更；可查看失败原因后重试，重叠/相邻分区会被调度拦截。
                </div>
                <div v-else class="text-[11.5px] text-[var(--ink-soft)]">
                  worker 正在逐分区重写并校验行数，全部通过后才会统一发布快照版本。
                </div>
                <div class="mt-3 flex flex-wrap gap-2">
                  <RouterLink
                    v-if="selected.snapshot_id"
                    :to="{ path: '/snapshots', query: { table: String(selected.table_id) } }"
                    class="bp-btn bp-btn--solid"
                  >
                    查看快照 diff
                  </RouterLink>
                  <button
                    v-if="selected.status === 'failed' && auth.canWrite"
                    class="bp-btn bp-btn--danger"
                    data-test="retry-job"
                    @click="retry"
                  >
                    重试任务
                  </button>
                  <RouterLink
                    v-if="selected.table_id"
                    :to="`/tables/${selected.table_id}`"
                    class="bp-btn"
                  >
                    返回分区
                  </RouterLink>
                </div>
              </div>
            </div>
          </div>
        </template>
        <EmptyState
          v-else
          title="尚未选择任务"
          description="左侧任务台账会列出当前湖表的合并任务；提交计划后这里会自动出现执行进度。"
        />
      </main>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import PageHeader from '../components/common/PageHeader.vue'
import ErrorNotice from '../components/common/ErrorNotice.vue'
import EmptyState from '../components/common/EmptyState.vue'
import StatusTag from '../components/common/StatusTag.vue'
import JobProgress from '../components/common/JobProgress.vue'
import PartitionRunTable from '../components/common/PartitionRunTable.vue'
import { useJobStore } from '../stores/jobStore'
import { useAuthStore } from '../stores/authStore'
import { useJobPolling } from '../hooks/useJobPolling'
import type { ScheduleTable, ScheduleWaitingJob } from '../types/job'

const route = useRoute()
const router = useRouter()
const jobStore = useJobStore()
const auth = useAuthStore()

const selected = computed(() => jobStore.selected)
const selectedId = computed(() => selected.value?.id ?? null)
const schedule = computed(() => jobStore.schedule)

const occupiedTables = computed<ScheduleTable[]>(() =>
  (schedule.value?.tables ?? []).filter((table) => table.occupied.length > 0),
)
const waitingEntries = computed<{ table: ScheduleTable; job: ScheduleWaitingJob }[]>(() =>
  (schedule.value?.tables ?? []).flatMap((table) => table.waiting.map((job) => ({ table, job }))),
)
const snapshotTables = computed<ScheduleTable[]>(() =>
  (schedule.value?.tables ?? []).filter((table) => table.latest_snapshot !== null),
)

const polling = useJobPolling(async () => jobStore.load(), 1200)

onMounted(async () => {
  await jobStore.load()
  const queryJob = Number(route.query.job)
  if (queryJob) jobStore.select(queryJob)
  if (jobStore.hasActive) polling.start()
})

watch(
  () => jobStore.hasActive,
  (active) => {
    if (active) polling.start()
    else polling.stop()
  },
)

function select(jobId: number) {
  jobStore.select(jobId)
  if (route.query.job !== String(jobId)) {
    router.replace({ path: '/jobs', query: { ...route.query, job: String(jobId) } })
  }
}

async function refresh() {
  await jobStore.load()
}

async function retry() {
  if (!selected.value) return
  await jobStore.retry(selected.value.id)
  polling.start()
}
</script>
