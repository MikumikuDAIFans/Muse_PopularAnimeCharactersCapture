<template>
  <div class="dashboard">
    <h1>仪表盘</h1>
    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-value">{{ stats.total_posts || 0 }}</div>
        <div class="stat-label">已下载帖子</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">{{ stats.total_tasks || 0 }}</div>
        <div class="stat-label">总任务数</div>
      </div>
      <div class="stat-card">
        <div class="stat-value running">{{ stats.running_tasks || 0 }}</div>
        <div class="stat-label">运行中</div>
      </div>
      <div class="stat-card">
        <div class="stat-value completed">{{ stats.completed_tasks || 0 }}</div>
        <div class="stat-label">已完成</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">{{ formatBytes(stats.total_download_bytes || 0) }}</div>
        <div class="stat-label">已下载数据</div>
      </div>
      <div class="stat-card">
        <div class="stat-value failed">{{ stats.failed_tasks || 0 }}</div>
        <div class="stat-label">失败任务</div>
      </div>
    </div>

    <div class="recent-activity">
      <h3>最近活动</h3>
      <div v-if="stats.recent_activity?.length" class="activity-list">
        <div
          v-for="(item, i) in stats.recent_activity"
          :key="i"
          class="activity-item"
          :class="item.level.toLowerCase()"
        >
          <span class="activity-level">{{ item.level }}</span>
          <span class="activity-msg">{{ item.message }}</span>
        </div>
      </div>
      <div v-else class="empty">暂无活动记录</div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import api from '@/api/client'

const stats = ref({})

function formatBytes(bytes) {
  if (!bytes) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i]
}

async function loadStats() {
  try {
    stats.value = await api.getStats()
  } catch (e) {
    console.error('Failed to load stats', e)
  }
}

onMounted(loadStats)
</script>

<style scoped>
.dashboard h1 { margin-bottom: 20px; }
.stats-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 16px; margin-bottom: 32px; }
.stat-card { background: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,.1); }
.stat-value { font-size: 28px; font-weight: bold; margin-bottom: 4px; }
.stat-value.running { color: #2196f3; }
.stat-value.completed { color: #4caf50; }
.stat-value.failed { color: #f44336; }
.stat-label { color: #888; font-size: 13px; }
.recent-activity { background: #fff; padding: 20px; border-radius: 8px; }
.recent-activity h3 { margin-bottom: 12px; }
.activity-list { display: flex; flex-direction: column; gap: 8px; max-height: 300px; overflow-y: auto; }
.activity-item { display: flex; gap: 10px; font-size: 13px; padding: 6px 8px; border-radius: 4px; background: #fafafa; }
.activity-level { font-weight: bold; min-width: 50px; }
.activity-item.info .activity-level { color: #2196f3; }
.activity-item.warn .activity-level { color: #ff9800; }
.activity-item.error .activity-level { color: #f44336; }
.empty { color: #888; font-size: 14px; padding: 20px; text-align: center; }
</style>