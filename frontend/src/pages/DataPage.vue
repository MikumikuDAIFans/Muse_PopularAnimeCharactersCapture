<template>
  <div class="data-page">
    <h1>数据浏览</h1>
    <div class="tabs">
      <button :class="{ active: tab === 'posts' }" @click="tab = 'posts'">帖子</button>
      <button :class="{ active: tab === 'tags' }" @click="tab = 'tags'">标签</button>
      <button :class="{ active: tab === 'characters' }" @click="tab = 'characters'">角色榜单</button>
      <button :class="{ active: tab === 'emerging' }" @click="tab = 'emerging'">新兴角色榜</button>
    </div>

    <!-- 帖子列表 -->
    <div v-if="tab === 'posts'" class="panel">
      <div class="filter-bar">
        <input v-model="postFilter" placeholder="标签筛选" @keyup.enter="loadPosts" />
        <button @click="loadPosts">搜索</button>
        <button @click="exportPosts('json')">导出JSON</button>
        <button @click="exportPosts('csv')">导出CSV</button>
      </div>
      <table class="data-table">
        <thead><tr><th>ID</th><th>MD5</th><th>标签数</th><th>评分</th><th>格式</th><th>大小</th><th>下载</th></tr></thead>
        <tbody>
          <tr v-for="post in posts" :key="post.id">
            <td>{{ post.id }}</td>
            <td class="mono">{{ post.md5 || '-' }}</td>
            <td>{{ post.tag_count }}</td>
            <td>{{ post.score }}</td>
            <td>{{ post.file_ext }}</td>
            <td>{{ formatBytes(post.file_size) }}</td>
            <td>{{ post.file_verified ? '✓' : '-' }}</td>
          </tr>
        </tbody>
      </table>
      <div class="pagination">
        <button @click="page--; loadPosts()" :disabled="page <= 1">上一页</button>
        <span>第 {{ page }} 页</span>
        <button @click="page++; loadPosts()" :disabled="!hasMore">下一页</button>
      </div>
    </div>

    <!-- 标签列表 -->
    <div v-if="tab === 'tags'" class="panel">
      <div class="filter-bar">
        <select v-model="tagCategory">
          <option value="">全部</option>
          <option value="character">角色</option>
          <option value="copyright">作品</option>
          <option value="artist">画师</option>
          <option value="general">通用</option>
        </select>
        <input v-model="tagSearch" placeholder="标签名" @keyup.enter="loadTags" />
        <button @click="loadTags">搜索</button>
      </div>
      <table class="data-table">
        <thead><tr><th>标签名</th><th>类别</th><th>帖子数</th></tr></thead>
        <tbody>
          <tr v-for="tag in tags" :key="tag.id">
            <td>{{ tag.name }}</td>
            <td><span :class="'cat-' + tag.category">{{ tag.category }}</span></td>
            <td>{{ tag.post_count }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- 角色榜单 -->
    <div v-if="tab === 'characters'" class="panel">
      <div class="filter-bar">
        <label>最低帖子数: <input v-model.number="charMinCount" type="number" min="0" /></label>
        <label>近期(月): <input v-model.number="charMonths" type="number" min="1" /></label>
        <label>Top N: <input v-model.number="charTopN" type="number" min="1" /></label>
        <button @click="loadTopCharacters">刷新</button>
        <button @click="exportChars('json')">导出JSON</button>
        <button @click="exportChars('csv')">导出CSV</button>
        <button @click="openDatasetExport(datasetCharacter || (topChars[0] && topChars[0].character_tag))">样本导出器</button>
      </div>
      <table class="data-table">
        <thead><tr><th>#</th><th>角色标签</th><th>作品</th><th>帖子数</th><th>近期帖子</th><th>热度分</th><th>操作</th></tr></thead>
        <tbody>
          <tr v-for="(char, i) in topChars" :key="i">
            <td>{{ i + 1 }}</td>
            <td class="tag-cell">{{ char.character_tag }}</td>
            <td class="tag-cell">{{ char.copyrights?.join(', ') || '-' }}</td>
            <td>{{ char.post_count }}</td>
            <td>{{ char.recent_post_count }}</td>
            <td>{{ char.popularity_score }}</td>
            <td><button @click="openDatasetExport(char.character_tag)">导出样本</button></td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- 新兴角色榜 -->
    <div v-if="tab === 'emerging'" class="panel">
      <div class="filter-bar">
        <label>最低总帖数: <input v-model.number="emergingMinCount" type="number" min="0" /></label>
        <label>最低近期帖数: <input v-model.number="emergingMinRecent" type="number" min="0" /></label>
        <label>最大年龄(天): <input v-model.number="emergingMaxAge" type="number" min="1" /></label>
        <label>Top N: <input v-model.number="emergingTopN" type="number" min="1" /></label>
        <button @click="loadEmergingCharacters">刷新</button>
        <button @click="exportEmerging('json')">导出JSON</button>
        <button @click="exportEmerging('csv')">导出CSV</button>
        <button @click="openDatasetExport(emergingChars[0] && emergingChars[0].character_tag)">样本导出器</button>
      </div>
      <div class="filter-bar dataset-export">
        <span class="dataset-status">目标：筛掉古早常青角色，保留年龄较小且最近 6 个月活跃的新兴角色。刷新会读取当前参数对应的榜单结果，必要时自动重算。</span>
      </div>
      <table class="data-table">
        <thead><tr><th>#</th><th>角色标签</th><th>当前年龄(天)</th><th>近期帖数</th><th>近期占比</th><th>新兴分</th><th>首次出现</th><th>作品</th><th>操作</th></tr></thead>
        <tbody>
          <tr v-for="char in emergingChars" :key="char.rank + '-' + char.character_tag">
            <td>{{ char.rank }}</td>
            <td class="tag-cell">{{ char.character_tag }}</td>
            <td>{{ computeAgeDays(char.first_seen_at) }}</td>
            <td>{{ char.recent_post_count }}</td>
            <td>{{ Number(char.recent_ratio || 0).toFixed(3) }}</td>
            <td>{{ Number(char.growth_score || 0).toFixed(3) }}</td>
            <td>{{ formatDate(char.first_seen_at) }}</td>
            <td class="tag-cell">{{ char.copyrights?.join(', ') || '-' }}</td>
            <td><button @click="openDatasetExport(char.character_tag)">导出样本</button></td>
          </tr>
        </tbody>
      </table>
      <div v-if="emergingStatus" class="dataset-status">{{ emergingStatus }}</div>
      <div class="dataset-status">年龄按当前时间减去首次出现时间计算。</div>
    </div>

    <div v-if="showDatasetModal" class="modal-overlay" @click.self="showDatasetModal = false">
      <div class="modal">
        <h3>导出训练样本</h3>
        <div class="form-group">
          <label>角色标签</label>
          <input v-model="datasetCharacter" placeholder="hatsune_miku" />
        </div>
        <div class="form-group">
          <label>样本数</label>
          <input v-model.number="datasetLimit" type="number" min="1" max="1000" />
        </div>
        <div class="form-group checkbox-line">
          <label><input v-model="datasetDownload" type="checkbox" /> 下载图片</label>
        </div>
        <div class="form-group checkbox-line">
          <label><input v-model="datasetClean" type="checkbox" /> 清空该角色旧样本后再导出</label>
        </div>
        <div v-if="datasetStatus" class="dataset-status">{{ datasetStatus }}</div>
        <div class="form-actions">
          <button type="button" @click="showDatasetModal = false">关闭</button>
          <button type="button" class="btn-primary" @click="exportDataset">开始导出</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import api from '@/api/client'

const tab = ref('posts')
const posts = ref([])
const tags = ref([])
const topChars = ref([])
const emergingChars = ref([])
const postFilter = ref('')
const tagCategory = ref('')
const tagSearch = ref('')
const charMinCount = ref(50)
const charMonths = ref(6)
const charTopN = ref(200)
const datasetCharacter = ref('')
const datasetLimit = ref(20)
const datasetDownload = ref(true)
const datasetClean = ref(true)
const datasetStatus = ref('')
const showDatasetModal = ref(false)
const emergingMinCount = ref(50)
const emergingMinRecent = ref(10)
const emergingMaxAge = ref(1095)
const emergingTopN = ref(200)
const emergingStatus = ref('')
const page = ref(1)
const hasMore = ref(false)

async function loadPosts() {
  try {
    const params = { page: page.value, page_size: 20 }
    if (postFilter.value) params.tag = postFilter.value
    const res = await api.listPosts(params)
    posts.value = res.items
    hasMore.value = res.has_more
  } catch (e) { console.error(e) }
}

async function loadTags() {
  try {
    const params = { page_size: 100 }
    if (tagCategory.value) params.category = tagCategory.value
    if (tagSearch.value) params.name = tagSearch.value
    const res = await api.listTags(params)
    tags.value = res.items
  } catch (e) { console.error(e) }
}

async function loadTopCharacters() {
  try {
    const res = await api.getTopCharacters({
      n: charTopN.value,
      recent_months: charMonths.value,
      min_count: charMinCount.value,
    })
    topChars.value = res.characters
    if (!datasetCharacter.value && topChars.value.length) {
      datasetCharacter.value = topChars.value[0].character_tag
    }
  } catch (e) { console.error(e) }
}

async function loadEmergingCharacters() {
  try {
    emergingStatus.value = '加载中...'
    const res = await api.getEmergingCharacters({
      n: emergingTopN.value,
      min_count: emergingMinCount.value,
      min_recent_count: emergingMinRecent.value,
      max_age_days: emergingMaxAge.value,
    })
    emergingChars.value = res.characters
    emergingStatus.value = `已加载 ${res.total_count} 条新兴角色`
  } catch (e) {
    console.error(e)
    emergingStatus.value = '加载失败: ' + (e.response?.data?.detail || e.message)
  }
}

async function exportChars(format) {
  try {
    const res = await api.exportCharacters({
      n: charTopN.value,
      recent_months: charMonths.value,
      min_count: charMinCount.value,
      format,
    })
    const blob = new Blob([format === 'csv' ? res : JSON.stringify(res, null, 2)], {
      type: format === 'csv' ? 'text/csv' : 'application/json',
    })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `characters_top${charTopN.value}.${format}`
    a.click()
    URL.revokeObjectURL(url)
  } catch (e) { console.error(e) }
}

async function exportDataset() {
  if (!datasetCharacter.value) {
    datasetStatus.value = '请先填写角色标签'
    return
  }
  datasetStatus.value = '导出中...'
  try {
    const res = await api.exportDataset({
      character_tag: datasetCharacter.value,
      limit: datasetLimit.value,
      download_images: datasetDownload.value,
      clean_target_dir: datasetClean.value,
    })
    datasetStatus.value = `已导出 ${res.exported_count} 条到 ${res.dataset_dir}`
  } catch (e) {
    datasetStatus.value = '导出失败: ' + (e.response?.data?.detail || e.message)
  }
}

function openDatasetExport(characterTag) {
  if (characterTag) datasetCharacter.value = characterTag
  datasetStatus.value = ''
  showDatasetModal.value = true
}

function downloadBlob(content, filename, type) {
  const blob = new Blob([content], { type })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.click()
  URL.revokeObjectURL(url)
}

function exportPosts(format) {
  if (!posts.value.length) return
  if (format === 'json') {
    downloadBlob(JSON.stringify(posts.value, null, 2), `posts_page_${page.value}.json`, 'application/json')
    return
  }
  const header = ['id', 'md5', 'tag_count', 'score', 'file_ext', 'file_size', 'file_verified']
  const lines = [
    header.join(','),
    ...posts.value.map(post => header.map(key => JSON.stringify(post[key] ?? '')).join(',')),
  ]
  downloadBlob(lines.join('\n'), `posts_page_${page.value}.csv`, 'text/csv')
}

function exportEmerging(format) {
  if (!emergingChars.value.length) return
  if (format === 'json') {
    const payload = emergingChars.value.map(char => ({
      ...char,
      character_age_days: computeAgeDays(char.first_seen_at),
    }))
    downloadBlob(JSON.stringify(payload, null, 2), `emerging_top_${emergingTopN.value}.json`, 'application/json')
    return
  }
  const header = ['rank', 'character_tag', 'character_age_days', 'recent_post_count', 'recent_ratio', 'growth_score', 'first_seen_at', 'copyrights']
  const lines = [
    header.join(','),
    ...emergingChars.value.map(char => header.map(key => {
      let value
      if (key === 'copyrights') value = (char.copyrights || []).join('|')
      else if (key === 'character_age_days') value = computeAgeDays(char.first_seen_at)
      else value = (char[key] ?? '')
      return JSON.stringify(value)
    }).join(',')),
  ]
  downloadBlob(lines.join('\n'), `emerging_top_${emergingTopN.value}.csv`, 'text/csv')
}

function formatBytes(bytes) {
  if (!bytes) return '-'
  const k = 1024; const sizes = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i]
}

function formatDate(value) {
  if (!value) return '-'
  return new Date(value).toLocaleDateString('zh-CN')
}

function computeAgeDays(value) {
  if (!value) return '-'
  const firstSeen = new Date(value)
  const now = new Date()
  const diffMs = now.getTime() - firstSeen.getTime()
  return Math.max(0, Math.floor(diffMs / (24 * 60 * 60 * 1000)))
}

onMounted(() => { loadPosts(); loadTags(); loadTopCharacters(); loadEmergingCharacters() })
</script>

<style scoped>
h1 { margin-bottom: 16px; }
.tabs { display: flex; gap: 8px; margin-bottom: 16px; }
.tabs button { padding: 8px 16px; border: 1px solid #ddd; background: #fff; border-radius: 4px; cursor: pointer; }
.tabs button.active { background: #1976d2; color: #fff; border-color: #1976d2; }
.panel { background: #fff; padding: 16px; border-radius: 8px; }
.filter-bar { display: flex; gap: 8px; margin-bottom: 16px; align-items: center; flex-wrap: wrap; }
.filter-bar input, .filter-bar select { padding: 6px 10px; border: 1px solid #ddd; border-radius: 4px; }
.filter-bar button { padding: 6px 12px; border: 1px solid #ddd; border-radius: 4px; background: #fff; cursor: pointer; }
.filter-bar button:hover { background: #f5f5f5; }
.dataset-export { background: #fafafa; border: 1px solid #eee; border-radius: 6px; padding: 10px; }
.dataset-status { color: #666; font-size: 12px; }
.modal-overlay { position: fixed; inset: 0; background: rgba(0,0,0,.45); display: flex; align-items: center; justify-content: center; z-index: 1000; }
.modal { width: 420px; max-width: calc(100vw - 32px); background: #fff; border-radius: 10px; padding: 20px; color: #222; }
.modal h3 { margin-bottom: 16px; }
.form-group { margin-bottom: 12px; }
.form-group label { display: block; margin-bottom: 6px; font-size: 13px; color: #555; }
.form-group input { width: 100%; padding: 8px 10px; border: 1px solid #ddd; border-radius: 4px; }
.checkbox-line label { display: flex; gap: 8px; align-items: center; }
.form-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 16px; }
.btn-primary { background: #1976d2; color: #fff; border-color: #1976d2; }
.data-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.data-table th { background: #f5f5f5; text-align: left; padding: 8px 12px; border-bottom: 2px solid #ddd; }
.data-table td { padding: 8px 12px; border-bottom: 1px solid #eee; }
.data-table tr:hover td { background: #fafafa; }
.mono { font-family: monospace; font-size: 11px; }
.tag-cell { font-size: 12px; }
.cat-character { background: #e8f5e9; color: #2e7d32; padding: 2px 6px; border-radius: 3px; }
.cat-copyright { background: #e3f2fd; color: #1565c0; padding: 2px 6px; border-radius: 3px; }
.cat-artist { background: #fff3e0; color: #e65100; padding: 2px 6px; border-radius: 3px; }
.pagination { display: flex; gap: 12px; align-items: center; margin-top: 16px; }
.pagination button { padding: 6px 12px; border: 1px solid #ddd; border-radius: 4px; background: #fff; cursor: pointer; }
</style>
