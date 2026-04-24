<template>
  <div class="task-page">
    <div class="header">
      <h1>任务管理</h1>
      <button class="btn-primary" @click="showCreate = true">+ 新建任务</button>
    </div>

    <!-- 统计 -->
    <div class="task-stats">
      <span>总计: {{ taskStats.total_count }}</span>
      <span class="pending">待处理: {{ taskStats.pending }}</span>
      <span class="running">运行: {{ taskStats.running }}</span>
      <span class="completed">完成: {{ taskStats.completed }}</span>
      <span class="failed">失败: {{ taskStats.failed }}</span>
    </div>

    <!-- 任务列表 -->
    <div class="task-list" v-if="tasks.length">
      <div v-for="task in tasks" :key="task.id" class="task-card">
        <div class="task-info">
          <div class="task-name">{{ task.name }}</div>
          <div class="task-meta">
            <span class="task-type">{{ task.task_type }}</span>
            <span class="task-time">{{ formatTime(task.created_at) }}</span>
          </div>
        </div>
        <div class="task-status" :class="task.status">{{ task.status }}</div>
        <div class="task-progress">
          <div class="progress-bar">
            <div class="progress-fill" :style="{ width: (task.progress * 100) + '%' }"></div>
          </div>
          <span class="progress-text">{{ task.processed_count }} / {{ task.total_count }}</span>
        </div>
        <div class="task-actions">
          <button v-if="task.status === 'pending'" @click="startTask(task.id)">启动</button>
          <button v-if="task.status === 'running'" @click="pauseTask(task.id)">暂停</button>
          <button v-if="task.status === 'paused'" @click="startTask(task.id)">继续</button>
          <button v-if="task.status === 'running' || task.status === 'paused'" class="btn-danger" @click="stopTask(task.id)">停止</button>
          <button class="btn-danger" @click="deleteTask(task.id)">删除</button>
        </div>
      </div>
    </div>
    <div v-else class="empty">暂无任务</div>

    <!-- 新建任务弹窗 -->
    <div v-if="showCreate" class="modal-overlay" @click.self="showCreate = false">
      <div class="modal">
        <h3>新建任务</h3>
        <form @submit.prevent="createTask">
          <div class="form-group">
            <label>任务名称</label>
            <input v-model="form.name" required placeholder="例如: 帖子爬取-2024" />
          </div>
          <div class="form-group">
            <label>任务类型</label>
            <select v-model="form.task_type" required>
              <option value="posts">帖子爬取</option>
              <option value="tags">标签爬取</option>
              <option value="characters">角色分析</option>
            </select>
          </div>
          <div v-if="form.task_type === 'posts'" class="form-group">
            <label>起始ID</label>
            <input v-model.number="form.params.start_id" type="number" placeholder="例如: 10576339" />
          </div>
          <div v-if="form.task_type === 'posts'" class="form-group">
            <label>结束ID</label>
            <input v-model.number="form.params.end_id" type="number" placeholder="例如: 10908849" />
          </div>
          <div v-if="form.task_type === 'posts'" class="form-group">
            <label>标签筛选（可选）</label>
            <input v-model="form.params.tags" placeholder="例如: character:frieren" />
          </div>
          <div v-if="form.task_type === 'characters'" class="form-group">
            <label>最低帖子数</label>
            <input v-model.number="form.params.min_post_count" type="number" value="50" />
          </div>
          <div class="form-actions">
            <button type="button" @click="showCreate = false">取消</button>
            <button type="submit" class="btn-primary">创建</button>
          </div>
        </form>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import api from '@/api/client'

const tasks = ref([])
const taskStats = ref({ total_count: 0, pending: 0, running: 0, completed: 0, failed: 0 })
const showCreate = ref(false)
const form = reactive({
  name: '',
  task_type: 'posts',
  params: { start_id: null, end_id: null, tags: '', min_post_count: 50 },
})

async function loadTasks() {
  try {
    tasks.value = await api.listTasks({ page_size: 50 })
  } catch (e) {
    console.error(e)
  }
}

async function loadStats() {
  try {
    taskStats.value = await api.getTaskStats()
  } catch (e) {
    console.error(e)
  }
}

async function createTask() {
  try {
    const params = { ...form.params }
    if (form.task_type === 'posts') {
      params.start_id = form.params.start_id
      params.end_id = form.params.end_id
      if (form.params.tags) params.tags = form.params.tags.split(',').map(t => t.trim())
    }
    await api.createTask({ name: form.name, task_type: form.task_type, params })
    showCreate.value = false
    form.name = ''
    form.params = { start_id: null, end_id: null, tags: '', min_post_count: 50 }
    await loadTasks()
    await loadStats()
  } catch (e) {
    alert('创建失败: ' + e.message)
  }
}

async function startTask(id) { await api.startTask(id); await loadTasks(); await loadStats() }
async function pauseTask(id) { await api.pauseTask(id); await loadTasks(); await loadStats() }
async function stopTask(id) { await api.stopTask(id); await loadTasks(); await loadStats() }
async function deleteTask(id) {
  if (!confirm('确认删除此任务？')) return
  await api.deleteTask(id)
  await loadTasks()
  await loadStats()
}

function formatTime(ts) {
  if (!ts) return '-'
  return new Date(ts).toLocaleString('zh-CN')
}

onMounted(() => { loadTasks(); loadStats() })
</script>

<style scoped>
.header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }
.task-stats { display: flex; gap: 20px; font-size: 14px; margin-bottom: 16px; }
.task-stats span { padding: 4px 10px; border-radius: 4px; background: #f0f0f0; }
.task-stats .running { color: #2196f3; }
.task-stats .completed { color: #4caf50; }
.task-stats .failed { color: #f44336; }
.task-list { display: flex; flex-direction: column; gap: 12px; }
.task-card { background: #fff; padding: 16px; border-radius: 8px; display: flex; gap: 16px; align-items: center; }
.task-info { flex: 1; }
.task-name { font-weight: bold; margin-bottom: 4px; }
.task-meta { display: flex; gap: 12px; font-size: 12px; color: #888; }
.task-type { background: #e3f2fd; color: #1976d2; padding: 2px 6px; border-radius: 3px; }
.task-status { padding: 4px 10px; border-radius: 4px; font-size: 13px; font-weight: bold; }
.task-status.pending { background: #fff3e0; color: #e65100; }
.task-status.running { background: #e3f2fd; color: #1565c0; }
.task-status.completed { background: #e8f5e9; color: #2e7d32; }
.task-status.failed { background: #ffebee; color: #c62828; }
.task-status.cancelled { background: #f5f5f5; color: #757575; }
.task-progress { flex: 1; }
.progress-bar { height: 6px; background: #eee; border-radius: 3px; margin-bottom: 4px; }
.progress-fill { height: 100%; background: #4caf50; border-radius: 3px; transition: width .3s; }
.progress-text { font-size: 12px; color: #888; }
.task-actions { display: flex; gap: 8px; }
button { padding: 5px 12px; border-radius: 4px; border: 1px solid #ddd; background: #fff; cursor: pointer; font-size: 13px; }
button:hover { background: #f5f5f5; }
.btn-primary { background: #1976d2; color: #fff; border-color: #1976d2; }
.btn-primary:hover { background: #1565c0; }
.btn-danger { color: #f44336; border-color: #f44336; }
.btn-danger:hover { background: #ffebee; }
.modal-overlay { position: fixed; inset: 0; background: rgba(0,0,0,.4); display: flex; align-items: center; justify-content: center; }
.modal { background: #fff; padding: 24px; border-radius: 8px; width: 480px; max-width: 90vw; }
.modal h3 { margin-bottom: 16px; }
.form-group { margin-bottom: 12px; }
.form-group label { display: block; font-size: 13px; color: #666; margin-bottom: 4px; }
.form-group input, .form-group select { width: 100%; padding: 8px 10px; border: 1px solid #ddd; border-radius: 4px; font-size: 14px; }
.form-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 16px; }
.empty { text-align: center; padding: 40px; color: #888; }
</style>