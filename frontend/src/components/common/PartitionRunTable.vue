<template>
  <div class="bp-frame" data-test="partition-run-table">
    <div class="bp-frame__head">
      <span class="label">分区重写与行数校验</span>
      <span class="num text-[10.5px] text-[var(--ink-soft)]">{{ partitions.length }} 分区</span>
    </div>
    <div class="max-h-[280px] overflow-auto">
      <table class="bp-table">
        <thead>
          <tr>
            <th>分区</th>
            <th>状态</th>
            <th class="text-right">重写</th>
            <th class="text-right">产出</th>
            <th class="text-right">Σrows 前 → 后</th>
            <th>行数校验</th>
            <th class="text-right">快照版本</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="part in partitions" :key="part.partition_value" :data-test="`partition-run-${part.partition_value}`">
            <td class="font-mono text-[11.5px]">{{ partitionKey }}={{ part.partition_value }}</td>
            <td>
              <span class="bp-tag" :class="tone(part.status)">{{ label(part.status) }}</span>
            </td>
            <td class="num">{{ part.rewritten_files }}</td>
            <td class="num">{{ part.produced_files }}</td>
            <td class="num">{{ part.rows_before.toLocaleString() }} → {{ part.rows_after.toLocaleString() }}</td>
            <td>
              <span class="bp-tag" :class="part.row_conserved ? 'bp-tag--ok' : 'bp-tag--hot'">
                {{ part.row_conserved ? '守恒' : '不守恒' }}
              </span>
            </td>
            <td class="num">{{ part.snapshot_version ? `v${part.snapshot_version}` : '—' }}</td>
          </tr>
        </tbody>
      </table>
    </div>
    <div
      v-for="part in failedParts"
      :key="`error-${part.partition_value}`"
      class="border-t border-[var(--rule-soft)] px-3.5 py-2 text-[11px] text-[var(--rust)]"
    >
      {{ partitionKey }}={{ part.partition_value }}：{{ part.error }}
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { JobPartitionTask, PartitionTaskStatus } from '../../types/job'

const props = defineProps<{ partitions: JobPartitionTask[]; partitionKey: string }>()

const STATUS_LABELS: Record<PartitionTaskStatus, string> = {
  pending: '待重写',
  rewriting: '重写中',
  validated: '已校验',
  published: '已发布',
  failed: '失败',
}

const failedParts = computed(() => props.partitions.filter((part) => part.status === 'failed' && part.error))

function label(status: PartitionTaskStatus): string {
  return STATUS_LABELS[status] ?? status
}

function tone(status: PartitionTaskStatus): string {
  if (status === 'published') return 'bp-tag--ok'
  if (status === 'validated') return 'bp-tag--watch'
  if (status === 'rewriting') return 'bp-tag--watch'
  if (status === 'failed') return 'bp-tag--hot'
  return 'bp-tag--muted'
}
</script>
