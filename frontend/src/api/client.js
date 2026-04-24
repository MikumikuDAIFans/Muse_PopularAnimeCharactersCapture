import axios from 'axios'

const client = axios.create({
  baseURL: '/api',
  timeout: 30000,
})

client.interceptors.response.use(
  res => res.data,
  err => {
    console.error('API Error:', err)
    return Promise.reject(err)
  }
)

export const api = {
  // 健康检查
  health: () => axios.get('/health').then(res => res.data),

  // 任务
  createTask: (data) => client.post('/tasks', data),
  listTasks: (params) => client.get('/tasks', { params }),
  getTask: (id) => client.get(`/tasks/${id}`),
  deleteTask: (id) => client.delete(`/tasks/${id}`),
  startTask: (id) => client.post(`/tasks/${id}/start`),
  pauseTask: (id) => client.post(`/tasks/${id}/pause`),
  stopTask: (id) => client.post(`/tasks/${id}/stop`),
  getTaskLogs: (id, params) => client.get(`/tasks/${id}/logs`, { params }),
  getTaskStats: () => client.get('/tasks/stats'),

  // 项目
  listProjects: () => client.get('/projects'),
  createProject: (data) => client.post('/projects', data),

  // 帖子
  listPosts: (params) => client.get('/posts', { params }),
  getPost: (id) => client.get(`/posts/${id}`),
  getPostStats: () => client.get('/posts/stats'),

  // 标签
  listTags: (params) => client.get('/tags', { params }),
  listCharacterTags: (params) => client.get('/tags/character', { params }),

  // 角色
  listCharacters: (params) => client.get('/characters', { params }),
  getTopCharacters: (params) => client.get('/characters/top', { params }),
  getEmergingCharacters: (params) => client.get('/characters/emerging', { params }),
  buildEmergingCharacters: (params) => client.post('/characters/build-emerging', null, { params }),

  // 导出
  exportCharacters: (params) => client.get('/export/characters', {
    params,
    responseType: params?.format === 'csv' ? 'text' : 'json',
  }),
  exportDataset: (data) => client.post('/datasets/export', data),

  // 统计
  getStats: () => client.get('/stats'),
}

export default api
